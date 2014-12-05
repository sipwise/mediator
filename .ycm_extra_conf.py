##########################################################################
# Simple ycm_extra_conf.py example                                       #
# Copyright (C) <2013>  Onur Aslan  <onur@onur.im>                       #
#                                                                        #
# This file is loaded by default. Place your own .ycm_extra_conf.py to   #
# project root to override this.                                         #
#                                                                        #
# This program is free software: you can redistribute it and/or modify   #
# it under the terms of the GNU General Public License as published by   #
# the Free Software Foundation, either version 3 of the License, or      #
# (at your option) any later version.                                    #
#                                                                        #
# This program is distributed in the hope that it will be useful,        #
# but WITHOUT ANY WARRANTY; without even the implied warranty of         #
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the          #
# GNU General Public License for more details.                           #
#                                                                        #
# You should have received a copy of the GNU General Public License      #
# along with this program.  If not, see <http://www.gnu.org/licenses/>.  #
##########################################################################


flags = [
'-g',
'-Wall',
'-O3',
'-std=c99',
'-D_POSIX_SOURCE',
'-D_GNU_SOURCE',
'-I/usr/include/glib-2.0',
'-I/usr/lib/x86_64-linux-gnu/glib-2.0/include',
'-pthread',
'-I/usr/include/glib-2.0',
'-I/usr/lib/x86_64-linux-gnu/glib-2.0/include',
'-I/usr/include/mysql',
'-DBIG_JOINS=1',
'-fno-strict-aliasing',
'-g',
'-DNDEBUG',
'-pthread',

'-x', 'c'
]

# youcompleteme is calling this function to get flags
# You can also set database for flags. Check: JSONCompilationDatabase.html in
# clang-3.2-doc package
def FlagsForFile( filename ):
  return {
    'flags': flags,
    'do_cache': True
  }
