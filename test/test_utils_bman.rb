#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/thread'
require 'higgs/utils/bman'
require 'logger'
require 'test/unit'

module Higgs::Test
  class UtilsBackupManagerTest < Test::Unit::TestCase
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    STORAGE_ITEMS = (ENV['STORAGE_ITEMS'] || '100').to_i
    WARM_START_ITEMS = (ENV['WARM_START_ITEMS'] || '1000').to_i
    MAX_ITEM_BYTES = (ENV['MAX_ITEM_BYTES'] || '16384').to_i
    ITEM_CHARS = ('A'..'Z').to_a + ('a'..'z').to_a

    def setup
      srand(0)
      @from_dir = 'bman_from'
      @from_name = 'foo'
      @from = File.join(@from_dir, @from_name)
      @to_dir = 'bman_to'
      @to_name = 'bar'
      FileUtils.rm_rf(@from_dir) #  for debug
      FileUtils.mkdir_p(@from_dir)
      FileUtils.rm_rf(@to_dir)  # for debug
      FileUtils.mkdir_p(@to_dir)
      @jlog_rotate_service_uri = 'druby://localhost:14142'
      @from_st = Storage.new(@from,
                             :jlog_rotate_max => 0,
                             :jlog_rotate_service_uri => @jlog_rotate_service_uri,
                             :logger => proc{|path|
                               logger = Logger.new(path, 1)
                               logger.level = Logger::DEBUG
                               logger
                             })
      @bman = Utils::BackupManager.new(:from => @from,
                                       :to_dir => @to_dir,
                                       :to_name => @to_name,
                                       :jlog_rotate_service_uri => @jlog_rotate_service_uri,
                                       :verbose => $DEBUG ? 2 : 0)
    end

    def teardown
      @from_st.shutdown
      DRb.stop_service          # Why cannot each service be stopped?
      FileUtils.rm_rf(@from_dir) unless $DEBUG
      FileUtils.rm_rf(@to_dir) unless $DEBUG
    end

    def test_backup_index
      @bman.backup_index
      assert((File.exist? File.join(@to_dir, @to_name + '.idx')))
    end

    def test_backup_data
      @bman.backup_data
      assert(FileUtils.cmp(@from + '.tar', File.join(@to_dir, @to_name + '.tar')))
    end

    def test_rotate_jlog_0
      assert_equal(0, Storage.rotate_entries(@from + '.jlog').length)
    end

    def test_rotate_jlog_1
      @bman.rotate_jlog
      assert_equal(1, Storage.rotate_entries(@from + '.jlog').length)
    end

    def test_rotate_jlog_2
      @bman.rotate_jlog
      @bman.rotate_jlog
      assert_equal(2, Storage.rotate_entries(@from + '.jlog').length)
    end

    def test_rotate_jlog_10
      10.times do
        @bman.rotate_jlog
      end
      assert_equal(10, Storage.rotate_entries(@from + '.jlog').length)
    end

    def test_backup_jlog_0
      @bman.backup_jlog
      assert_equal(0, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)
    end

    def test_backup_jlog_1
      @bman.rotate_jlog
      @bman.backup_jlog
      assert_equal(1, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)
    end

    def test_backup_jlog_2
      @bman.rotate_jlog
      @bman.rotate_jlog
      @bman.backup_jlog
      assert_equal(2, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)
    end

    def test_backup_jlog_10
      10.times do
        @bman.rotate_jlog
      end
      @bman.backup_jlog
      assert_equal(10, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)
    end

    def test_clean_jlog
      @bman.rotate_jlog
      @bman.rotate_jlog
      @bman.rotate_jlog
      @bman.backup_jlog

      assert_equal(3, Storage.rotate_entries(@from + '.jlog').length)
      assert_equal(3, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)

      @bman.clean_jlog
      assert_equal(0, Storage.rotate_entries(@from + '.jlog').length)
      assert_equal(0, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)
    end

    def test_clean_jlog_delete_backup
      @bman.rotate_jlog
      @bman.rotate_jlog
      @bman.rotate_jlog
      @bman.backup_jlog
      @bman.rotate_jlog

      assert_equal(4, Storage.rotate_entries(@from + '.jlog').length)
      assert_equal(3, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)

      @bman.clean_jlog
      assert_equal(1, Storage.rotate_entries(@from + '.jlog').length)
      assert_equal(0, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)
    end

    def test_clean_jlog_no_backup_no_delete
      @bman.rotate_jlog
      @bman.rotate_jlog
      @bman.rotate_jlog

      assert_equal(3, Storage.rotate_entries(@from + '.jlog').length)
      assert_equal(0, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)

      @bman.clean_jlog
      assert_equal(3, Storage.rotate_entries(@from + '.jlog').length)
      assert_equal(0, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)
    end

    def update_storage(options)
      count = 0
      while (options[:spin_lock])
        count += 1
        options[:end_of_warm_up].start if (count == WARM_START_ITEMS)

        write_list = []
        ope = [ :write, :system_properties, :custom_properties, :delete ][rand(3)]
        key = rand(STORAGE_ITEMS)
        case (ope)
        when :write
          value = rand(256).chr * rand(MAX_ITEM_BYTES)
          write_list << [ ope, key, value ]
        when :system_properties
          next unless (@from_st.key? key)
          write_list << [ ope, key, { 'string_only' => [ true, false ][rand(2)] } ]
        when :custom_properties
          next unless (@from_st.key? key)
          value = ITEM_CHARS[rand(ITEM_CHARS.size)] * rand(MAX_ITEM_BYTES)
          write_list << [ ope, key, { 'foo' => value } ]
        when :delete
          next unless (@from_st.key? key)
          write_list << [ ope, key ]
        else
          raise "unknown operation: #{ope}"
        end
        @from_st.write_and_commit(write_list)
      end
    end
    private :update_storage

    def test_recover_and_verify_and_clean_jlog
      options = {
        :end_of_warm_up => Latch.new,
        :spin_lock => true
      }
      t = Thread.new{ update_storage(options) }

      options[:end_of_warm_up].wait
      @bman.backup_index
      @bman.backup_data
      options[:spin_lock] = false
      t.join

      @bman.rotate_jlog
      @bman.backup_jlog
      @bman.recover
      @bman.verify
      @bman.clean_jlog

      assert(FileUtils.cmp(@from + '.tar', File.join(@to_dir, @to_name + '.tar')))
      assert_equal(Index.new.load(@from + '.idx').to_h,
                   Index.new.load(File.join(@to_dir, @to_name + '.idx')).to_h)
      assert_equal(0, Storage.rotate_entries(@from + '.jlog').length)
      assert_equal(0, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)
    end

    def test_online_backup
      options = {
        :end_of_warm_up => Latch.new,
        :spin_lock => true
      }
      t = Thread.new{ update_storage(options) }

      options[:end_of_warm_up].wait
      @bman.online_backup
      options[:spin_lock] = false
      t.join

      assert((File.file? File.join(@to_dir, @to_name + '.tar')))
      assert((File.file? File.join(@to_dir, @to_name + '.idx')))
      assert_equal(0, Storage.rotate_entries(File.join(@to_dir, @to_name + '.jlog')).length)
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
