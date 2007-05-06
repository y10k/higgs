#!/usr/local/bin/ruby

require 'drb'
require 'fileutils'
require 'higgs/index'
require 'higgs/storage'
require 'test/unit'

module Higgs::Test
  class OnlineBackupTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    STORAGE_ITEMS = 100
    COMMIT_ITEMS = 10
    MAX_ITEM_BYTES = 1024 * 16
    LEAST_COMMITS_PER_ROTATION = 8
    UPTIME_SECONDS = 3

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

      @jlog_rotate_service_uri = 'druby://localhost:14142'

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
                       :jlog_rotate_service_uri => @jlog_rotate_service_uri)
      begin
        FileUtils.touch(@start_latch)
        until (File.exist? @stop_latch)
          write_list = []
          ope = [ :write, :update_properties, :delete ][rand(3)]
          key = rand(STORAGE_ITEMS)
          case (ope)
          when :write
            value = rand(256).chr * rand(MAX_ITEM_BYTES)
            write_list << [ ope, key, value ]
          when :update_properties
            next unless (st.key? key)
            value = rand(256).chr * rand(MAX_ITEM_BYTES)
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
          sleep(0.001)
        end
        st.verify
      ensure
        st.shutdown
      end
    end
    private :run_backup_storage

    def test_online_backup
      pid = fork{ run_backup_storage }
      begin
        until (File.exist? @start_latch)
          sleep(0.001)
        end
        jlog_rotate_service = DRbObject.new_with_uri(@jlog_rotate_service_uri)
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
          sleep(0.001)
        end

        # step 3: rotate journal log
        jlog_rotate_service.call(true)

        # step 4: backup old journal logs
        for path in Storage.rotate_entries("#{@backup_name}.jlog")
          FileUtils.cp(path, File.join(@restore_dir, File.basename(path)))
          FileUtils.rm(path)
        end

        # step 4: recover from backup
        Storage.recover(@restore_name)

        # recovered files are same as original files.
        assert(FileUtils.cmp("#{@backup_name}.tar", "#{@restore_name}.tar"), 'tar')
        assert_equal(Index.new.load("#{@backup_name}.idx").to_h,
                     Index.new.load("#{@restore_name}.idx").to_h, 'idx')
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
