#include <hiredis/hiredis.h>
#include <assert.h>

#include "medredis.h"
#include "config.h"

#define PBXSUFFIX "_pbx-1"
#define XFERSUFFIX "_xfer-1"

#define medredis_check_reply(con, cmd, reply, err) do { \
    if (!(reply) && !(con)->ctx) { \
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
} while(0);


// TODO: replace by GQueue https://developer.gnome.org/glib/stable/glib-Double-ended-Queues.html
typedef struct medredis_command {
	char **argv;
	size_t argc;
	struct medredis_command *next;
} medredis_command_t;

typedef struct medredis_con {
	redisContext *ctx;
	medredis_command_t *command_queue;
	unsigned int append_counter;
} medredis_con_t;

typedef struct {
	med_callid_t *entries;
	uint64_t index;
} medredis_cidlist_t;

static medredis_con_t *con = NULL;

/**********************************************************************/
static med_entry_t *medredis_reply_to_entry(redisReply *reply) {
	med_entry_t *entry;

	if (reply->elements != 8) {
		L_ERROR("Invalid number of redis reply elements for acc record, expected 8, got %lu\n",
			reply->elements);
		return NULL;
	}
	entry = (med_entry_t*)malloc(sizeof(med_entry_t));
	memset(entry, 0, sizeof(med_entry_t));

	g_strlcpy(entry->sip_code,   reply->element[0]->str, sizeof(entry->sip_code));
	g_strlcpy(entry->sip_reason, reply->element[1]->str, sizeof(entry->sip_reason));
	g_strlcpy(entry->sip_method, reply->element[2]->str, sizeof(entry->sip_method));
	g_strlcpy(entry->callid,     reply->element[3]->str, sizeof(entry->callid));
	g_strlcpy(entry->timestamp,  reply->element[4]->str, sizeof(entry->timestamp));
	entry->unix_timestamp =      atof(reply->element[5]->str);
	g_strlcpy(entry->src_leg,    reply->element[6]->str, sizeof(entry->src_leg));
	g_strlcpy(entry->dst_leg,    reply->element[7]->str, sizeof(entry->dst_leg));
	entry->valid = 1;
	entry->redis = 1;

	L_DEBUG("Converted record with cid '%s' and method '%s'\n", entry->callid, entry->sip_method);

	return entry;
}

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
static void medredis_free_command(medredis_command_t **cmd) {
	if (!cmd)
		return;
	
	if ((*cmd)->argv) {
		for (size_t i = 0; i < (*cmd)->argc; ++i)
			free((*cmd)->argv[i]);
		free((*cmd)->argv);
	}
	free(*cmd);
	*cmd = NULL;
}

/**********************************************************************/
static int medredis_push_command(medredis_con_t *con, size_t argc, char **argv) {

    medredis_command_t *cmd = NULL;
    medredis_command_t *tmp = NULL;
    
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
    cmd->next = NULL;

    if (!con->command_queue) {
        con->command_queue = cmd;
    } else {
        tmp = con->command_queue;
        while (tmp->next)
            tmp = tmp->next;
        tmp->next = cmd;
    }

    return 0;

err:
    medredis_free_command(&cmd);
    return -1;
}

/**********************************************************************/
static medredis_command_t* medredis_shift_command(medredis_con_t *con) {
    medredis_command_t *cmd;

    cmd = con->command_queue;
    if (cmd) {
        con->command_queue = cmd->next;
    }

    return cmd;
}

/**********************************************************************/
static medredis_command_t* medredis_pop_command(medredis_con_t *con) {
    medredis_command_t *current;
    medredis_command_t *ret;

    current = con->command_queue;
    if (!current)
        return NULL;
    if (!current->next) {
    	con->command_queue = NULL;
    	return current;
    }
    while (current->next->next) {
    	current = current->next;
    }
    ret = current->next;
    current->next = NULL;
    return ret;
}

/**********************************************************************/
static redisReply *medredis_command(medredis_con_t *con, const char* cmd) {
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
static redisReply *medredis_command_argv(medredis_con_t *con, size_t argc, char **argv) {
    redisReply *reply = redisCommandArgv(con->ctx, argc, (const char**)argv, NULL);
    if (con->ctx->err == REDIS_ERR_EOF) {
        if (medredis_init() != 0) {
            L_ERROR("Failed to reconnect to redis db\n");
            medredis_cleanup();
            return NULL;
        }
        reply = redisCommandArgv(con->ctx, argc, (const char**)argv, NULL);
    }
    return reply;
}

/**********************************************************************/
static int medredis_append_command_argv(medredis_con_t *con, size_t argc, char **argv, int queue) {
	int ret;

    if (queue > 0 && medredis_push_command(con, argc, argv) != 0) {
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
            return ret;
        }
        ret = redisAppendCommandArgv(con->ctx, argc, (const char**)argv, NULL);
    }
    if (!con->ctx->err) {
        con->append_counter++;
    }
    return ret;
}

/**********************************************************************/
static int medredis_get_reply(medredis_con_t *con, redisReply **reply) {
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
        }
        // take commands from oldest to newest and re-do again,
        // but don't queue them once again in retry-mode
        while ((cmd = medredis_shift_command(con))) {
            L_DEBUG("re-queueing appended command\n");
            if (medredis_append_command_argv(con, cmd->argc, cmd->argv, 0) != 0) {
                L_ERROR("Failed to re-queue redis command");
                return -1;
            }
            medredis_free_command(&cmd);
        }
        ret = redisGetReply(con->ctx, (void**)reply);
        if (con->ctx->err != REDIS_ERR_EOF) {
            con->append_counter--;
        }
    } else {
        L_DEBUG("Redis get_reply successful, popping command\n");
        cmd = medredis_pop_command(con);
        medredis_free_command(&cmd);
        con->append_counter--;
    }
    return ret;
}


/**********************************************************************/
static void medredis_consume_replies(medredis_con_t *con) {
	medredis_command_t *cmd;
	redisReply *reply;
	while (con->append_counter) {
		redisGetReply(con->ctx, (void**)&reply);
		if (reply) {
			con->append_counter--;	
		} else {
			con->append_counter = 0;
		}
		medredis_free_reply(&reply);
	}
	while ((cmd = medredis_shift_command(con))) {
		medredis_free_command(&cmd);
	}
}

/**********************************************************************/
int medredis_init() {
    struct timeval tv;
    redisReply *reply;

    tv.tv_sec = 1;
    tv.tv_usec = 0;

    reply = NULL;

    L_DEBUG("Connecting to redis %s:%d/%d\n",
    	config_redis_host, config_redis_port, config_redis_db);

    con = (medredis_con_t*)malloc(sizeof(medredis_con_t));
    if (!con) {
    	L_ERROR("Failed to allocate memory for redis connection\n");
    	goto err;
    }

    con->command_queue = NULL;
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
        medredis_check_reply(con, "AUTH", reply, err);
        medredis_free_reply(&reply);
    }

    reply = redisCommand(con->ctx, "PING");
    medredis_check_reply(con, "PING", reply, err);
    medredis_free_reply(&reply);

    reply = redisCommand(con->ctx, "SELECT %i", config_redis_db);
    medredis_check_reply(con, "SELECT", reply, err);
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
		free(con);
		con = NULL;
	}
}

/**********************************************************************/
static void medredis_add_cid(void *key, void *val, void *data) {
	medredis_cidlist_t *list = (medredis_cidlist_t*)data;
	L_DEBUG("Adding cid %s [%s] at index %"PRIu64"\n", (char*)key, (char*)val, list->index);
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
		reply = medredis_command(con, buffer);
		medredis_check_reply(con, buffer, reply, err);

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


            // strip leading "acc:entry::" and trailing ":<time_hires>"
            cid = strdup(entry->str + strlen("acc:entry::"));
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

	} while (cursor);

	*count = g_hash_table_size(cid_table);
	entries = (med_callid_t*)malloc(*count * sizeof(*entries));
	if (!entries) {
		L_ERROR("Failed to allocate memory for callid list\n");
		goto err;
	}
	memset(entries, 0, *count * sizeof(*entries));
	cid_list.entries = entries;
	cid_list.index = 0;
	g_hash_table_foreach(cid_table, medredis_add_cid, &cid_list);
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

	size_t entry_argc = 10;
	char *entry_argv[10];

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


    if (medredis_append_command_argv(con, entry_argc, entry_argv, 1) != 0) {
    	L_ERROR("Failed to append command to fetch key\n");
    }
    free(key);
}

/**********************************************************************/
static gint medredis_sort_entry(gconstpointer a, gconstpointer b) {
	med_entry_t *aa = (med_entry_t*)a;
	med_entry_t *bb = (med_entry_t*)b;

	if (aa->unix_timestamp == bb->unix_timestamp)
		return 0;
	else if (aa->unix_timestamp < bb->unix_timestamp)
		return -1;
	else
		return 1;
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

	redisReply *reply;

	char *cids[3];
	GList *records;
	GList *keys;

	size_t i;
	uint8_t has_bye = 0;
	uint8_t has_inv_200 = 0;
	
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
		if (medredis_append_command_argv(con, cid_set_argc, cid_set_argv, 1) != 0) {
			L_ERROR("Failed to append redis command to fetch entries for cid '%s'\n", callid->value);
			goto err;
		}
		free(cid);
	}

	for (i = 0; i < 3; ++i) {
		if (medredis_get_reply(con, &reply) != 0) {
			L_ERROR("Failed to get redis reply for command to fetch entries for cid '%s'\n", callid->value);
			goto err;	
		}
		medredis_check_reply(con, "smembers for cid", reply, err);
		medredis_dump_reply(reply);

		if (reply->type != REDIS_REPLY_ARRAY) {
            L_ERROR("Invalid reply type for scan, expected array\n");
            goto err;
        }
        if (!reply->elements) {
        	L_DEBUG("No matching entries for cid '%s' at suffix idx %lu\n", callid->value, i);
        	continue;
        }

        for (size_t j = 0; j < reply->elements; ++j) {
        	char *key = strdup(reply->element[j]->str);
        	if (!key) {
        		L_ERROR("Failed to allocate memory for redis key\n");
        		goto err;
        	}
        	L_DEBUG("Putting key '%s' to keys list\n", key);
        	keys = g_list_append(keys, key);

        }
        medredis_free_reply(&reply);
    }

    L_DEBUG("Appending all keys to redis command\n");
    g_list_foreach(keys, medredis_append_key, NULL);
    do {
    	med_entry_t *e;
    	L_DEBUG("Fetching next reply record\n");
    	if (medredis_get_reply(con, &reply) != 0) {
    		L_ERROR("Failed to get reply from redis\n");
    		goto err;
    	}
    	if (!reply)
    		break;
    	medredis_check_reply(con, "get reply", reply, err);
    	medredis_dump_reply(reply);

    	e = medredis_reply_to_entry(reply);
    	if (!e) {
    		L_ERROR("Failed to convert redis reply to entry\n");
    		goto err;
    	}
    	records = g_list_insert_sorted(records, e, medredis_sort_entry);

    } while(1);

    *count = g_list_length(records);
    *entries = (med_entry_t*)malloc(*count * sizeof(med_entry_t));
    if (!*entries) {
    	L_ERROR("Failed to allocate memory for entries\n");
    	goto err;
    }
    memset(*entries, 0, *count * sizeof(med_entry_t));
    i = 0;
    for (GList *l = records; l; l = l->next) {
    	med_entry_t *s = (med_entry_t*)l->data;
    	med_entry_t *d = &(*entries)[i++];

    	L_DEBUG("Copying record with cid='%s', method='%s', code='%s'",
    		s->callid, s->sip_method, s->sip_code);

    	if (s->sip_method[0] == 'I' && s->sip_method[1] == 'N' && s->sip_method[2] == 'V' &&
    			s->sip_code[0] == '2') {
    		has_inv_200 |= 1;
    	} else if (s->sip_method[0] == 'B' && s->sip_method[1] == 'Y' && s->sip_method[2] == 'E') {
    		has_bye |= 1;
    	}

    	memcpy(d, s, sizeof(med_entry_t));
    	free(s);

    	L_DEBUG("Added entry with cid '%s' and method '%s'\n", d->callid, d->sip_method);
    }

    if (has_inv_200 && !has_bye) {
    	L_DEBUG("Found incomplete call with cid '%s', skipping...\n", callid->value);
    	free(*entries);
    	*entries = NULL;
    	*count = 0;
    }

    g_list_free(records);
    g_list_free(keys);

	medredis_consume_replies(con);

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
	medredis_consume_replies(con);
	return -1;


}



/**********************************************************************/
static int medredis_rename_entries(const char *callid, const char *type) {
	char *argv[3];
	char *cids[3];
	char buffer[512];
	char buffer_src[512];
	char buffer_dst[512];
	size_t i, j;
	redisReply *reply = NULL;

	memset(cids, 0, sizeof(cids));
    cids[0] = strdup(callid);
    if (!cids[0]) {
    	L_ERROR("Failed to allocate memory for callid\n");
    	goto err;
    }
    snprintf(buffer, sizeof(buffer), "%s%s", callid, PBXSUFFIX);
    cids[1] = strdup(buffer);
    if (!cids[1]) {
    	L_ERROR("Failed to allocate memory for callid with pbxsuffix\n");
    	goto err;
    }
    snprintf(buffer, sizeof(buffer), "%s%s", callid, XFERSUFFIX);
    cids[2] = strdup(buffer);
    if (!cids[2]) {
    	L_ERROR("Failed to allocate memory for callid with xfersuffix\n");
    	goto err;
    }

	// first, rename entry keys
    for (i = 0; i < 3; ++i) {
		char *cid = cids[i];
		snprintf(buffer_src, sizeof(buffer_src), "acc:cid::%s", cid);
		argv[0] = "SMEMBERS";
		argv[1] = buffer_src;
		reply = medredis_command_argv(con, 2, argv);
		medredis_check_reply(con, "smembers cid cleanup", reply, err);

		for (j = 0; j < reply->elements; ++j) {
			char *key = reply->element[j]->str;
			char *time_hires = strrchr(key, ':');
			time_hires++;
			snprintf(buffer_src, sizeof(buffer_src), "%s", key);
			snprintf(buffer_dst, sizeof(buffer_dst), "acc:entry_%s::%s:%s", type, cid, time_hires);
			argv[0] = "RENAME";
			argv[1] = buffer_src;
			argv[2] = buffer_dst;
			if (medredis_append_command_argv(con, 3, argv, 1) != 0) {
				L_ERROR("Failed to append redis command to rename entries for cid '%s'\n", callid);
				goto err;
			}

			argv[0] = "SREM";
			argv[1] = "acc:meth::INVITE";
			argv[2] = key;
			if (medredis_append_command_argv(con, 3, argv, 1) != 0) {
				L_ERROR("Failed to append redis command to remove key '%s' from acc:meth::INVITE\n", key);
				goto err;
			}
			argv[1] = "acc:meth::BYE";
			if (medredis_append_command_argv(con, 3, argv, 1) != 0) {
				L_ERROR("Failed to append redis command to remove key '%s' from acc:meth::BYE\n", key);
				goto err;
			}
		}
		medredis_free_reply(&reply);
	}

    // next, move away the overall acc:cid set to acc:cid_trash
    for (i = 0; i < 3; ++i) {
		char *cid = cids[i];
		snprintf(buffer_src, sizeof(buffer_src), "acc:cid::%s", cid);
		snprintf(buffer_dst, sizeof(buffer_dst), "acc:cid_%s::%s", type, cid);
		argv[0] = "RENAME";
		argv[1] = buffer_src;
		argv[2] = buffer_dst;
		// by design, we don't rename the content of the set, so although we'll rename acc:entry to acc:entry_bak,
		// the sets will still point to acc:entry
		if (medredis_append_command_argv(con, 3, argv, 1) != 0) {
			L_ERROR("Failed to append redis command to rename entries for cid '%s'\n", callid);
			goto err;
		}
	}

	L_DEBUG("Waiting for final replies for cid '%s' after cleanup\n", callid);
	medredis_consume_replies(con);

	for (i = 0; i < 3; ++i) {
		char *cid = cids[i];
		free(cid);
	}
	L_DEBUG("Done renaming '%s' for cleanup\n", callid);
	return 0;

err:

	medredis_consume_replies(con);

	for (i = 0; i < 3; ++i) {
		char *cid = cids[i];
		if (cid)
			free(cid);
	}
	medredis_free_reply(&reply);
	return -1;
}

/**********************************************************************/
int medredis_trash_entries(const char *callid) {
	return medredis_rename_entries(callid, "trash");
}

/**********************************************************************/
int medredis_backup_entries(const char *callid) {
	return medredis_rename_entries(callid, "backup");
}