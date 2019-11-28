#include <hiredis/hiredis.h>
#include <assert.h>

#include "medredis.h"
#include "medmysql.h"
#include "config.h"

#define PBXSUFFIX "_pbx-1"
#define XFERSUFFIX "_xfer-1"

#define medredis_check_reply(cmd, reply, err) do { \
    if (!(reply) && (!(con) || !(con)->ctx)) { \
        L_ERROR("Failed to perform redis query '%s': no connection to server\n", \
            (cmd)); \
        goto err; \
    } \
    if (!(reply)) { \
        L_ERROR("Failed to perform redis query '%s': %s\n", \
                (cmd), (con)->ctx->errstr); \
        goto err; \
    } \
    if ((reply)->type == REDIS_REPLY_ERROR) { \
        L_ERROR("Failed to perform redis query '%s': %s\n", \
                (cmd), (reply)->str); \
        goto err; \
    } \
} while(0)

#define SREM_KEY_LUA "redis.call('SREM', KEYS[1], KEYS[3]); if redis.call('SCARD', KEYS[1]) == 0 then redis.call('SREM', KEYS[2], KEYS[1]) end"

typedef struct {
    char **argv;
    size_t argc;
} medredis_command_t;

typedef struct {
    redisContext *ctx;
    GQueue *command_queue;
    unsigned int append_counter;
} medredis_con_t;

typedef struct {
    med_callid_t *entries;
    uint64_t index;
} medredis_cidlist_t;

static medredis_con_t *con = NULL;
static char medredis_srem_key_lua[41]; // sha-1 hex string

/**********************************************************************/
static void medredis_free_reply(redisReply **reply) {
    if (reply && *reply) {
        freeReplyObject(*reply);
        *reply = NULL;
    }
}

/**********************************************************************/
static void medredis_dump_reply(redisReply *reply) {
    size_t i;
    if (reply->type == REDIS_REPLY_STRING) {
        L_DEBUG("  %s\n", reply->str);
    } else if (reply->type == REDIS_REPLY_INTEGER) {
        L_DEBUG("  %lld\n", reply->integer);
    } else if (reply->type == REDIS_REPLY_NIL) {
        L_DEBUG("  <null>\n");
    } else if (reply->type == REDIS_REPLY_ARRAY) {
        L_DEBUG("printing %lu elements in array reply\n", reply->elements);
        for(i = 0; i < reply->elements; ++i) {
            medredis_dump_reply(reply->element[i]);
        }
    } else {
        L_DEBUG("  not printing invalid reply type\n");
    }
}

/**********************************************************************/
static void medredis_dump_command(size_t argc, char **argv) {
    L_DEBUG("Dumping command:\n");
    for (size_t i = 0; i < argc; ++i) {
        L_DEBUG("  %lu: %s", i, argv[i]);
    }
}

/**********************************************************************/
static void medredis_free_command(gpointer data) {
    medredis_command_t *cmd = (medredis_command_t*)data;
    if (!cmd)
        return;

    if (cmd->argv) {
        for (size_t i = 0; i < cmd->argc; ++i)
            free(cmd->argv[i]);
        free(cmd->argv);
    }
    free(cmd);
}

/**********************************************************************/
static int medredis_push_command(size_t argc, char **argv) {

    medredis_command_t *cmd = NULL;
    
    if (!argv)
        return 0;

    cmd = (medredis_command_t*)malloc(sizeof(medredis_command_t));
    if (!cmd) {
        L_ERROR("Failed to allocate memory for redis command\n");
        goto err;
    }
    cmd->argc = 0;
    cmd->argv = (char**)malloc(argc * sizeof(char*));
    if (!cmd->argv) {
        L_ERROR("Failed to allocate memory for redis query\n");
        goto err;    
    }
    for (size_t i = 0; i < argc; ++i) {
        cmd->argv[i] = (char*)malloc(strlen(argv[i]) + 1);
        strcpy(cmd->argv[i], argv[i]);
    }
    cmd->argc = argc;

    g_queue_push_tail(con->command_queue, cmd);

    return 0;

err:
    medredis_free_command(cmd);
    return -1;
}

/**********************************************************************/
static redisReply *medredis_command(const char* cmd) {
    L_DEBUG("Performing redis query '%s'\n", cmd);
    redisReply *reply = redisCommand(con->ctx, cmd);
    if (con->ctx->err == REDIS_ERR_EOF) {
        if (medredis_init() != 0) {
            L_ERROR("Failed to reconnect to redis db\n");
            medredis_cleanup();
            return NULL;
        }
        reply = redisCommand(con->ctx, cmd);
    }
    return reply;
}

/**********************************************************************/
static int medredis_append_command_argv(size_t argc, char **argv, int queue) {
    int ret;

    if (queue > 0 && medredis_push_command(argc, argv) != 0) {
        L_ERROR("Failed to queue redis command\n");
        return -1;
    }

    medredis_dump_command(argc, argv);

    ret = redisAppendCommandArgv(con->ctx, argc, (const char**)argv, NULL);

    // this should actually never happen, because if all replies
    // are properly consumed for the previous command, it won't send
    // out a new query until redisGetReply is called
    if (con->ctx->err == REDIS_ERR_EOF) {
        if (medredis_init() != 0) {
            L_ERROR("Failed to reconnect to redis db\n");
            medredis_cleanup();
            return -1;
        }
        ret = redisAppendCommandArgv(con->ctx, argc, (const char**)argv, NULL);
    }
    if (!con->ctx->err) {
        con->append_counter++;
    }
    return ret;
}

/**********************************************************************/
static int medredis_get_reply(redisReply **reply) {
    int ret;
    medredis_command_t *cmd;

    if (con->append_counter <= 0) {
        *reply = NULL;
        return 0;
    }

    *reply = NULL;
    ret = redisGetReply(con->ctx, (void**)reply);
    if (con->ctx->err == REDIS_ERR_EOF) {
        L_DEBUG("Redis connection is gone, try reconnect\n");
        con->append_counter = 0;
        if (medredis_init() != 0) {
            L_ERROR("Failed to reconnect to redis db\n");
            medredis_cleanup();
	    return -1;
        }
        // take commands from oldest to newest and re-do again,
        // but don't queue them once again in retry-mode
        while ((cmd = g_queue_pop_head(con->command_queue))) {
            L_DEBUG("re-queueing appended command\n");
            if (medredis_append_command_argv(cmd->argc, cmd->argv, 0) != 0) {
                L_ERROR("Failed to re-queue redis command");
                return -1;
            }
            medredis_free_command(cmd);
        }
        ret = redisGetReply(con->ctx, (void**)reply);
        if (con->ctx->err != REDIS_ERR_EOF) {
            con->append_counter--;
        }
    } else {
        L_DEBUG("Redis get_reply successful, un-queueing command\n");
        cmd = g_queue_pop_head(con->command_queue);
        medredis_free_command(cmd);
        con->append_counter--;
    }
    return ret;
}


/**********************************************************************/
static void medredis_consume_replies(void) {
    medredis_command_t *cmd;
    redisReply *reply;
    while (con->append_counter > 0) {
        redisGetReply(con->ctx, (void**)&reply);
        if (reply) {
            con->append_counter--;    
        } else {
            con->append_counter = 0;
        }
        medredis_free_reply(&reply);
    }
    while ((cmd = g_queue_pop_head(con->command_queue))) {
        medredis_free_command(cmd);
    }
}

/**********************************************************************/
static int medredis_remove_entry(char* key) {
    char *argv[2];

    // delete acc:entry::$cid:$timestamp
    argv[0] = "DEL";
    argv[1] = key;
    if (medredis_append_command_argv(2, argv, 1) != 0) {
        L_ERROR("Failed to append redis command to remove key '%s'\n", key);
        goto err;
    }

    medredis_consume_replies();
    return 0;

err:
    medredis_consume_replies();
    return -1;
}

/**********************************************************************/
static int medredis_remove_mappings(const char* cid, char* key) {
    char *argv[6];
    char buffer[512];

    snprintf(buffer, sizeof(buffer), "acc:cid::%s", cid);

    // delete cid from acc:cid::$cid mapping and handle acc::index::cid mapping
    argv[0] = "EVALSHA";
    argv[1] = medredis_srem_key_lua;
    argv[2] = "3";
    argv[3] = buffer;
    argv[4] = "acc::index::cid";
    argv[5] = key;
    if (medredis_append_command_argv(6, argv, 1) != 0) {
        L_ERROR("Failed to append redis command to remove mapping key '%s' from '%s'\n", key, argv[1]);
        goto err;
    }

    // delete cid from acc:meth::INVITE and acc:meth::BYE and handle acc::index::meth mappings
    argv[0] = "EVALSHA";
    argv[1] = medredis_srem_key_lua;
    argv[2] = "3";
    argv[3] = "acc:meth::INVITE";
    argv[4] = "acc::index::meth";
    argv[5] = key;
    if (medredis_append_command_argv(6, argv, 1) != 0) {
        L_ERROR("Failed to append redis command to remove mapping key '%s' from '%s'\n", key, argv[1]);
        goto err;
    }
    argv[3] = "acc:meth::BYE";
    if (medredis_append_command_argv(6, argv, 1) != 0) {
        L_ERROR("Failed to append redis command to remove mapping key '%s' from '%s'\n", key, argv[1]);
        goto err;
    }

    medredis_consume_replies();
    return 0;

err:
    medredis_consume_replies();
    return -1;
}

/**********************************************************************/
static int medredis_check_reply_string(med_entry_t *entry, redisReply *reply,
        char* element, const char* element_name, size_t element_size,
        int index,
        const char* cid, const char* key) {
    int ret;
    if (reply->element[index]->type != REDIS_REPLY_STRING) {
        L_WARNING("Received Redis reply type %i instead of %i (string) for %s field of cid '%s' using key '%s'\n",
                reply->element[index]->type, REDIS_REPLY_STRING, element_name, cid, key);
        entry->valid = 0;
        ret = -1;
    } else {
        g_strlcpy(element, reply->element[index]->str, element_size);
        ret = 0;
    }
    return ret;
}

/**********************************************************************/
static med_entry_t *medredis_reply_to_entry(redisReply *reply, const char* cid, char* key) {
    med_entry_t *entry;
    uint8_t all_null = 1;

    if (reply->elements != 9) {
        L_ERROR("Invalid number of redis reply elements for acc record with cid '%s' and key '%s', expected 9, got %lu, trashing record\n",
            cid, key, reply->elements);
        medredis_remove_mappings(cid, key);
        medredis_remove_entry(key);
        return NULL;
    }

    // check for all fields being null, as this is a special case where the entry
    // doesn't exist in redis at all (compared to cases where only a certain field might be null)
    for (int i = 0; i < (int) reply->elements; i++) {
        if (reply->element[i]->type != REDIS_REPLY_NIL) {
            all_null = 0;
            break;
        }
    }
    if (all_null) {
        L_WARNING("Redis entry does not exist for key '%s', trashing mappings", key);
        medredis_remove_mappings(cid, key);
        return NULL;
    }

    // verify call-id and trash if invalid
    if (reply->element[3]->type != REDIS_REPLY_STRING) {
        L_WARNING("Received Redis reply type %i instead of %i (string) for call-id field of cid '%s' using key '%s', trashing record\n",
                reply->element[3]->type, REDIS_REPLY_STRING, cid, key);
        medredis_remove_mappings(cid, key);
        medredis_remove_entry(key);
        return NULL;
    }

    entry = (med_entry_t*)malloc(sizeof(med_entry_t));
    memset(entry, 0, sizeof(med_entry_t));
    entry->valid = 1;
    entry->redis = 1;

    medredis_check_reply_string(entry, reply, entry->sip_code, "sip_code", sizeof(entry->sip_code), 0, cid, key);
    medredis_check_reply_string(entry, reply, entry->sip_reason, "sip_reason", sizeof(entry->sip_reason), 1, cid, key);
    medredis_check_reply_string(entry, reply, entry->sip_method, "sip_method", sizeof(entry->sip_method), 2, cid, key);
    medredis_check_reply_string(entry, reply, entry->callid, "callid", sizeof(entry->callid), 3, cid, key);
    if (medredis_check_reply_string(entry, reply, entry->timestamp, "timestamp", sizeof(entry->timestamp), 4, cid, key) != 0) {
        g_strlcpy(entry->timestamp, "0000-00-00 00:00:00", sizeof(entry->timestamp));
    }
    if (reply->element[5]->type != REDIS_REPLY_STRING) {
        L_WARNING("Received Redis reply type %i instead of %i (string) for unix_timestamp field of cid '%s' using key '%s'\n",
                reply->element[5]->type, REDIS_REPLY_STRING, cid, key);
        entry->valid = 0;
    } else {
        entry->unix_timestamp = atof(reply->element[5]->str);
    }
    medredis_check_reply_string(entry, reply, entry->src_leg, "src_leg", sizeof(entry->src_leg), 6, cid, key);
    medredis_check_reply_string(entry, reply, entry->dst_leg, "dst_leg", sizeof(entry->dst_leg), 7, cid, key);
    if (reply->element[8]->type != REDIS_REPLY_STRING) {
        L_DEBUG("Received Redis reply of cid '%s' using key '%s' doesn't have branch_id >>> old acc record version\n", cid, key);
        g_strlcpy(entry->branch_id, "", sizeof(entry->branch_id));
    } else {
        medredis_check_reply_string(entry, reply, entry->branch_id, "branch_id", sizeof(entry->branch_id), 8, cid, key);
    }
    
    L_DEBUG("Converted record with cid '%s' and method '%s'\n", entry->callid, entry->sip_method);

    return entry;
}


/**********************************************************************/
int medredis_init() {
    struct timeval tv;
    redisReply *reply;

    tv.tv_sec = 1;
    tv.tv_usec = 0;

    reply = NULL;

    medredis_cleanup();

    L_DEBUG("Connecting to redis %s:%d/%d\n",
        config_redis_host, config_redis_port, config_redis_db);

    con = (medredis_con_t*)malloc(sizeof(medredis_con_t));
    if (!con) {
        L_ERROR("Failed to allocate memory for redis connection\n");
        goto err;
    }

    con->command_queue = g_queue_new();
    con->append_counter = 0;
    con->ctx = redisConnectWithTimeout(config_redis_host, config_redis_port, tv);
    if (!con->ctx) {
        L_ERROR("Cannot open redis connection to %s:%d\n",
            config_redis_host, config_redis_port);
        goto err;
    }
    if (con->ctx->err) {
        L_ERROR("Cannot open redis connection to %s:%d: %s\n",
            config_redis_host, config_redis_port,
            con->ctx->errstr);
        goto err;
    }

    if (config_redis_pass) {
        reply = redisCommand(con->ctx, "AUTH %s", config_redis_pass);
        medredis_check_reply("AUTH", reply, err);
        medredis_free_reply(&reply);
    }

    reply = redisCommand(con->ctx, "PING");
    medredis_check_reply("PING", reply, err);
    medredis_free_reply(&reply);

    reply = redisCommand(con->ctx, "SELECT %i", config_redis_db);
    medredis_check_reply("SELECT", reply, err);
    medredis_free_reply(&reply);

    reply = redisCommand(con->ctx, "SCRIPT LOAD %s", SREM_KEY_LUA);
    medredis_check_reply("SELECT", reply, err);
    if (reply->type != REDIS_REPLY_STRING || reply->len >= sizeof(medredis_srem_key_lua)) {
        L_ERROR("Invalid reply from SCRIPT LOAD: %i/%lu\n", reply->type, (unsigned long) reply->len);
        goto err;
    }
    strcpy(medredis_srem_key_lua, reply->str);
    medredis_free_reply(&reply);
    
    L_DEBUG("Redis connection opened to %s:%d/%d\n",
        config_redis_host, config_redis_port, config_redis_db);

    return 0;

err:
    medredis_free_reply(&reply);
    if (con) {
        if (con->ctx) {
            redisFree(con->ctx);
            con->ctx = NULL;
        }
        free(con);
        con = NULL;
    }
    return -1;
}


/**********************************************************************/
void medredis_cleanup() {
    if (con) {
        if (con->ctx) {
            redisFree(con->ctx);
            con->ctx = NULL;
        }
        g_queue_free_full(con->command_queue, medredis_free_command);
        free(con);
        con = NULL;
    }
}

/**********************************************************************/
static void medredis_add_cid(void *key, void *val, void *data) {
    medredis_cidlist_t *list = (medredis_cidlist_t*)data;
    (void)val;
    L_DEBUG("Adding cid %s at index %"PRIu64"\n", (char*)key, list->index);
    g_strlcpy(list->entries[list->index].value, (char*)key, sizeof(list->entries[list->index].value));
    list->index++;
}

/**********************************************************************/
med_callid_t *medredis_fetch_callids(uint64_t *count) {
    unsigned int cursor = 0;
    size_t i = 0;
    redisReply *reply = NULL;
    char *cmd = "SSCAN acc:meth::INVITE %u COUNT 1000";
    med_callid_t *entries = NULL;
    GHashTable *cid_table;
    medredis_cidlist_t cid_list;
    char buffer[256];
    char *tmp;

    cid_table = g_hash_table_new_full(g_str_hash, g_str_equal, free, NULL);

    *count = 0;

    do {
        snprintf(buffer, sizeof(buffer), cmd, cursor);
        reply = medredis_command(buffer);
        medredis_check_reply(buffer, reply, err);

        if (reply->type != REDIS_REPLY_ARRAY) {
            L_ERROR("Invalid reply type for scan, expected array\n");
            goto err;
        }
        if (reply->elements != 2) {
            L_ERROR("Invalid number of reply elements for scan, expected 2, got %lu\n",
                    reply->elements);
            goto err;
        }

        if (reply->element[0]->type == REDIS_REPLY_STRING) {
            cursor = atol(reply->element[0]->str);
        } else if (reply->element[0]->type == REDIS_REPLY_INTEGER) {
            // should not happen, but play it safe
            cursor = reply->element[0]->integer;
        } else {
            L_ERROR("Invalid cursor type for scan, expected string or integer\n");
            goto err;
        }

        if (reply->element[1]->type != REDIS_REPLY_ARRAY) {
            L_ERROR("Invalid content type for scan, expected array\n");
            goto err;
        }
        if (reply->element[1]->elements == 0 && cursor == 0) {
            L_DEBUG("No matching entries found for scan\n");
            medredis_free_reply(&reply);
            break;
        }

        for (i= 0; i < reply->element[1]->elements; ++i) {
            char *cid;
            redisReply *entry = reply->element[1]->element[i];
            if (!entry) {
                L_ERROR("Invalid null entry at cursor result index %lu while scanning table\n", i);
                goto err;
            }
            if (entry->type != REDIS_REPLY_STRING) {
                L_ERROR("Invalid entry type at cursor result index %lu while scanning table, expected string\n", i);
                goto err;
            }

            L_DEBUG("Got entry '%s'\n", entry->str);


            // strip leading "acc:entry::" and trailing ":<time_hires>:<branch_id>"
            cid = strdup(entry->str + strlen("acc:entry::"));
            tmp = strrchr(cid, ':');
            if (tmp) {
                *tmp = '\0';
            }
            tmp = strrchr(cid, ':');
            if (tmp) {
                *tmp = '\0';
            }

            // strip (potentially chained) suffices
            if ((tmp = strstr(entry->str, PBXSUFFIX)) ||
                    (tmp = strstr(entry->str, XFERSUFFIX))) {
                *tmp = '\0';
            }

            if (g_hash_table_insert(cid_table, cid, cid)) {
                (*count)++;
            }

        }
        medredis_free_reply(&reply);
    } while (cursor && (*count) < 200000);

    *count = g_hash_table_size(cid_table);
    if (*count) {
        entries = (med_callid_t*)malloc(*count * sizeof(*entries));
        if (!entries) {
            L_ERROR("Failed to allocate memory for callid list\n");
            goto err;
        }
        cid_list.entries = entries;
        cid_list.index = 0;
        g_hash_table_foreach(cid_table, medredis_add_cid, &cid_list);
    }
    g_hash_table_destroy(cid_table);

    return entries;

err:
    if (reply)
        freeReplyObject(reply);
    *count = (uint64_t) -1;
    if (entries)
        free(entries);
    g_hash_table_destroy(cid_table);
    return NULL;
}    

/**********************************************************************/
static void medredis_append_key(gpointer data, gpointer user_data) {
    char *key = (char*)data;
    (void)user_data;

    L_DEBUG("Appending key '%s' to keys list\n", key);

    size_t entry_argc = 11;
    char *entry_argv[11];

    entry_argv[0] = "HMGET";
    entry_argv[1] = key;
    entry_argv[2] = "sip_code";
    entry_argv[3] = "sip_reason";
    entry_argv[4] = "method";
    entry_argv[5] = "callid";
    entry_argv[6] = "time";
    entry_argv[7] = "time_hires";
    entry_argv[8] = "src_leg";
    entry_argv[9] = "dst_leg";
    entry_argv[10] = "branch_id";


    if (medredis_append_command_argv(entry_argc, entry_argv, 1) != 0) {
        L_ERROR("Failed to append command to fetch key\n");
    }
}

/**********************************************************************/
static void medredis_free_keys_list(gpointer data) {
    char *key = (char*)data;
    free(key);
}

/**********************************************************************/
int medredis_fetch_records(med_callid_t *callid,
        med_entry_t **entries, uint64_t *count) {


    /*
        1. fetch from acc:cid::$cid
        2. fetch from acc:cid::$cidPBXSUFFIX
        3. fetch from acc:cid::$cidXFERSUFFIX
        4. combine all in list sorted by time_hires
        5. skip if INVITE/200 and no BYE
    */

    char buffer[512];

    uint8_t cid_set_argc;
    char *cid_set_argv[2];

    redisReply *reply = NULL;

    char *cids[3];
    GList *records;
    GList *keys;

    size_t i;
    
    cid_set_argc = 2;
    cid_set_argv[0] = "SMEMBERS";

    records = NULL;
    keys = NULL;


    memset(cids, 0, sizeof(cids));
    cids[0] = strdup(callid->value);
    if (!cids[0]) {
        L_ERROR("Failed to allocate memory for callid\n");
        goto err;
    }
    snprintf(buffer, sizeof(buffer), "%s%s", callid->value, PBXSUFFIX);
    cids[1] = strdup(buffer);
    if (!cids[1]) {
        L_ERROR("Failed to allocate memory for callid with pbxsuffix\n");
        goto err;
    }
    snprintf(buffer, sizeof(buffer), "%s%s", callid->value, XFERSUFFIX);
    cids[2] = strdup(buffer);
    if (!cids[2]) {
        L_ERROR("Failed to allocate memory for callid with xfersuffix\n");
        goto err;
    }

    *count = 0;
    *entries = NULL;

    L_DEBUG("Fetching records from redis\n");

    for (i = 0; i < 3; ++i) {
        char *cid = cids[i];
        snprintf(buffer, sizeof(buffer), "acc:cid::%s", cid);
        cid_set_argv[1] = buffer;
        if (medredis_append_command_argv(cid_set_argc, cid_set_argv, 1) != 0) {
            L_ERROR("Failed to append redis command to fetch entries for cid '%s'\n", callid->value);
            goto err;
        }
        free(cid);
        cids[i] = NULL;
    }

    for (i = 0; i < 3; ++i) {
        if (medredis_get_reply(&reply) != 0) {
            L_ERROR("Failed to get redis reply for command to fetch entries for cid '%s'\n", callid->value);
            goto err;    
        }
        medredis_check_reply("smembers for cid", reply, err);
        medredis_dump_reply(reply);

        if (reply->type != REDIS_REPLY_ARRAY) {
            medredis_free_reply(&reply);
            L_ERROR("Invalid reply type for scan, expected array\n");
            goto err;
        }
        if (!reply->elements) {
            medredis_free_reply(&reply);
            L_DEBUG("No matching entries for cid '%s' at suffix idx %lu\n", callid->value, i);
            continue;
        }

        for (size_t j = 0; j < reply->elements; ++j) {
            char *key = strdup(reply->element[j]->str);
            if (!key) {
                L_ERROR("Failed to allocate memory for redis key (cid '%s')\n", callid->value);
                goto err;
            }
            L_DEBUG("Putting key '%s' to keys list\n", key);
            keys = g_list_append(keys, key);

        }
        medredis_free_reply(&reply);
    }

    L_DEBUG("Appending all keys to redis command\n");
    g_list_foreach(keys, medredis_append_key, NULL);

    for (GList *l = keys; l; l = l->next) {
        med_entry_t *e;
        char *key = (char*)l->data;
        
        L_DEBUG("Fetching next reply record, query key was '%s'\n", key);
        if (medredis_get_reply(&reply) != 0) {
            L_ERROR("Failed to get reply from redis (cid '%s')\n", callid->value);
            goto err;
        }
        if (!reply)
            break;
        medredis_check_reply("get reply", reply, err);
        medredis_dump_reply(reply);

        e = medredis_reply_to_entry(reply, callid->value, key);
        if (!e) {
            L_WARNING("Failed to convert redis reply to entry (cid '%s')\n", callid->value);
            medredis_free_reply(&reply);
            continue;
        }
        medredis_free_reply(&reply);
        records = g_list_prepend(records, e);
        (*count)++;
    }

    if (!*count)
        goto no_entries;

    *entries = (med_entry_t*)malloc(*count * sizeof(med_entry_t));
    if (!*entries) {
        L_ERROR("Failed to allocate memory for entries (cid '%s')\n", callid->value);
        goto err;
    }
    i = 0;
    for (GList *l = records; l; l = l->next) {
        med_entry_t *s = (med_entry_t*)l->data;
        med_entry_t *d = &(*entries)[i++];

        L_DEBUG("Copying record with cid='%s', method='%s', code='%s'",
            s->callid, s->sip_method, s->sip_code);

        memcpy(d, s, sizeof(med_entry_t));
        free(s);

        L_DEBUG("Added entry with cid '%s' and method '%s'\n", d->callid, d->sip_method);
    }

no_entries:
    g_list_free(records);
    g_list_free_full(keys, medredis_free_keys_list);

    medredis_consume_replies();

    return 0;


err:
    if (reply)
        freeReplyObject(reply);
    *count = (uint64_t) -1;
    for (int i = 0; i < 3; ++i) {
        if (cids[i])
            free(cids[i]);
    }
    if (*entries)
        free(*entries);
    medredis_consume_replies();
    return -1;


}

/**********************************************************************/
static int medredis_cleanup_entries(med_entry_t *records, uint64_t count, const char *table) {
    char buffer[512];

    if (medmysql_insert_records(records, count, table) != 0) {
        L_CRITICAL("Failed to cleanup redis records\n");
        goto err;
    }

    for (uint64_t i = 0; i < count; ++i) {
        med_entry_t *e = &(records[i]);
        if (!e->redis)
            continue;

        const char *branch_id = e->branch_id;

        // do this at most twice: once with an empty branch ID, and once without a branch ID at all
        do {
            if (branch_id == NULL) {
                // Old ACC record version: it doesn't have branch_id
                snprintf(buffer, sizeof(buffer), "acc:entry::%s:%f", e->callid, e->unix_timestamp);
            }
            else {
                snprintf(buffer, sizeof(buffer), "acc:entry::%s:%f:%s", e->callid, e->unix_timestamp, branch_id);
            }

            L_DEBUG("Cleaning up redis entry for %s\n", buffer);
            if (medredis_remove_mappings(e->callid, buffer) != 0) {
                goto err;
            }
            if (medredis_remove_entry(buffer) != 0) {
                goto err;
            }

            // exit condition: if the branch ID is non-NULL but an empty string, try again with
            // a NULL branch ID.
            // otherwise (branch ID is NULL or a non-empty string) we break.
            if (branch_id == NULL || strlen(branch_id) != 0)
                break;

            branch_id = NULL;
        }
        while (1);
    }

    return 0;

err:
    return -1;
}

/**********************************************************************/
int medredis_trash_entries(med_entry_t *records, uint64_t count) {
    return medredis_cleanup_entries(records, count, "trash");
}

/**********************************************************************/
int medredis_backup_entries(med_entry_t *records, uint64_t count) {
    return medredis_cleanup_entries(records, count, "backup");
}
