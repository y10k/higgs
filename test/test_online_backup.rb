#!/usr/local/bin/ruby
# -*- coding: utf-8 -*-

require 'drb'
require 'fileutils'
require 'higgs/index'
require 'higgs/services'
require 'higgs/storage'
require 'logger'
require 'test/unit'

module Higgs::Test
  module OnlineBackupParams
    STORAGE_ITEMS = (ENV['STORAGE_ITEMS'] || '100').to_i
    COMMIT_ITEMS = (ENV['COMMIT_ITEMS'] || '10').to_i
    MAX_ITEM_BYTES = (ENV['MAX_ITEM_BYTES'] || '16384').to_i
    LEAST_COMMITS_PER_ROTATION = (ENV['LEAST_COMMITS_PER_ROTATION'] || '8').to_i
    UPTIME_SECONDS = (ENV['UPTIME_SECONDS'] || '3').to_i
    ITEM_CHARS = ('A'..'Z').to_a + ('a'..'z').to_a

    if ($DEBUG) then
      puts 'online backup test parameters...'
      for name in constants
        puts "#{name} = #{const_get(name)}"
      end
      puts ''
    end
  end

  class OnlineBackupTest < Test::Unit::TestCase
    include Higgs
    include OnlineBackupParams

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      srand(0)

      @backup_dir = 'onbk_test_backup'
      @backup_name = File.join(@backup_dir, 'foo')
      FileUtils.rm_rf(@backup_dir) # for debug
      FileUtils.mkdir_p(@backup_dir)

      @restore_dir = 'onbk_test_restore'
      @restore_name = File.join(@restore_dir, 'foo')
      FileUtils.rm_rf(@restore_dir) # for debug
      FileUtils.mkdir_p(@restore_dir)

      @remote_services_uri = 'druby://localhost:14142'

      @start_latch = File.join(@backup_dir, '.start')
      @stop_latch = File.join(@backup_dir, '.stop')
      @stopped_latch = File.join(@backup_dir, '.stopped')
      @end_latch = File.join(@backup_dir, '.end')
    end

    def teardown
      FileUtils.rm_rf(@backup_dir) unless $DEBUG
      FileUtils.rm_rf(@restore_dir) unless $DEBUG
    end

    def run_backup_storage
      st = Storage.new(@backup_name,
                       :jlog_rotate_max => 0,
                       :jlog_rotate_size => COMMIT_ITEMS * MAX_ITEM_BYTES * LEAST_COMMITS_PER_ROTATION,
                       :logger => proc{|path|
                         logger = Logger.new(path, 1)
                         logger.level = Logger::DEBUG
                         logger
                       })

      # step 0: storage starts with remote services.
      sv = RemoteServices.new(:remote_services_uri => @remote_services_uri,
                              :storage => st)

      begin
        FileUtils.touch(@start_latch)
        until (File.exist? @stop_latch)
          write_list = []
          ope = [ :write, :system_properties, :custom_properties, :delete ][rand(4)]
          key = rand(STORAGE_ITEMS)
          case (ope)
          when :write
            value = rand(256).chr * rand(MAX_ITEM_BYTES)
            write_list << [ ope, key, value ]
          when :system_properties
            next unless (st.key? key)
            write_list << [ ope, key, { 'string_only' => [ true, false ][rand(2)] } ]
          when :custom_properties
            next unless (st.key? key)
            value = ITEM_CHARS[rand(ITEM_CHARS.size)] * rand(MAX_ITEM_BYTES)
            write_list << [ ope, key, { 'foo' => value } ]
          when :delete
            next unless (st.key? key)
            write_list << [ ope, key ]
          else
            raise "unknown operation: #{ope}"
          end
          st.write_and_commit(write_list)
        end
        FileUtils.touch(@stopped_latch)
        until (File.exist? @end_latch)
          # spin lock
        end
        st.verify
      ensure
        st.shutdown
        sv.shutdown
      end
    end
    private :run_backup_storage

    def test_online_backup
      pid = fork{ run_backup_storage }
      DRb.start_service
      begin
        until (File.exist? @start_latch)
          # spin lock
        end
        sv = DRbObject.new_with_uri(@remote_services_uri)
        localhost_check_service = sv[:localhost_check_service_v1] or flunk
        localhost_check_service.call{|check|
          check.call
        }
        jlog_rotate_service = sv[:jlog_rotate_service_v1] or flunk
        sleep(UPTIME_SECONDS)

        # step 1: backup index
        jlog_rotate_service.call("#{@restore_name}.idx")

        # step 2: backup data
        FileUtils.cp("#{@backup_name}.tar", "#{@restore_name}.tar")

        # transactions are stopped for comparison between original
        # files and restored files. on real operation, transactions
        # are not stopped.
        FileUtils.touch(@stop_latch)
        until (File.exist? @stopped_latch)
          # spin lock
        end

        # step 3: rotate journal log
        jlog_rotate_service.call(true)

        # step 4: backup old journal logs
        for path in Storage.rotated_entries("#{@backup_name}.jlog")
          FileUtils.cp(path, File.join(@restore_dir, File.basename(path)))
          FileUtils.rm(path)
        end

        # step 4: recover from backup
        Storage.recover(@restore_name)

        # recovered files are same as original files.
        assert(FileUtils.cmp("#{@backup_name}.tar", "#{@restore_name}.tar"), 'DATA should be same.')
        assert(Index.new.load("#{@backup_name}.idx").to_h ==
               Index.new.load("#{@restore_name}.idx").to_h, 'INDEX should be same.')
      ensure
        FileUtils.touch(@stop_latch)
        FileUtils.touch(@end_latch)
        Process.waitpid(pid)
      end
      assert_equal(0, $?.exitstatus)

      st = Storage.new(@restore_name, :read_only => true)
      begin
        st.verify
      ensure
        st.shutdown
      end
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
