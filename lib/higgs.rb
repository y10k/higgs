# -*- coding: utf-8 -*-

require 'higgs/dbm'
require 'higgs/store'
require 'higgs/version'
require 'higgs/jcompat' if (RUBY_PLATFORM == 'java')

# pure ruby transactional storage compatible with unix TAR format
#
# == features
# * data format is compatible with unix TAR format.
# * data can have meta-data called `property'.
# * consistency of storage contents is always checked by hash value.
# * read-write transaction and read-only transaction are supported.
# * multi-version concurrency control. a read-write transaction
#   doesn't conflict with read-only transactions.
# * online-backup is supported.
# 
# == main classes
# [Higgs::Store] storage like pstore
# [Higgs::DBM] storage like dbm
# [Higgs::Utils::BackupManager] online backup utility (body of <tt>higgs_backup</tt> command)
#
# == robustness
# === case of no backup
# [REQUIREMENTS] default
# [NORMAL SHUTDOWN] OK, no recovery
# [PROCESS ABORT] OK, automatic recovery on read-write open
# [SYSTEM ABORT (OS abort)] NG, data is <em>NOT</em> consistent
#
# === case of online backup
# [REQUIREMENTS] open Higgs::StorageManager with these parameters:
#                <tt>jlog_rotate_max => 0</tt>,
#                <tt>remote_services_uri => "druby://<em>host</em>:<em>port</em>"</tt>,
#                and execute <tt>higgs_backup</tt> (see Higgs::Utils::BackupManager)
# [NORMAL SHUTDOWN] OK, no recovery
# [PROCESS ABORT] OK, automatic recovery on read-write open
# [SYSTEM ABORT (OS abort)] OK, need for <em>MANUAL</em> recovery from backup
#
# == license
# BSD style license.
#   :include:LICENSE
#
module Higgs
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
