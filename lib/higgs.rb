# -*- coding: utf-8 -*-

require 'higgs/dbm'
require 'higgs/store'
require 'higgs/version'
require 'higgs/jcompat' if (RUBY_PLATFORM == 'java')

# embedded key-value storage compatible with unit TAR format
#
# == features
# * pure ruby implementation (need for ruby 1.9).
# * data format is compatible with unix TAR format.
# * read-write transaction and read-only transaction are supported.
# * multi-version concurrency control. a read-write transaction
#   doesn't conflict with read-only transactions.
# * all data of key-value storage is verified by checksum.
# * online-backup is supported.
# 
# == main classes
# [Higgs::Store] storage like pstore
# [Higgs::DBM] storage like dbm
# [Higgs::Utils::BackupManager] online backup utility (body of <tt>higgs_backup</tt> command)
#
# == storage robustness
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
  def self.load_conf(path)
    require 'yaml'
    options = YAML.load_file(path)

    # see Higgs::Storage#initialize.
    if (log_level = options['logging_level']) then
      require 'logger'
      level = case(log_level)
              when 'debug', 'info', 'warn', 'error', 'fatal'
                Logger.const_get(options['logging_level'].upcase)
              else
                raise "unknown logging level: #{log_level}"
              end
      options[:logger] = proc{|path|
        logger = Logger.new(path, 1)
        logger.level = level
        logger
      }
    end

    options
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
