#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/dbm'
require 'higgs/storage'
require 'higgs/store'
require 'higgs/thread'
require 'higgs/tman'
require 'logger'
require 'test/unit'

module Higgs::Test
  module ReplicationTest
    include Higgs

    # for ident(1)
    CVS_ID = '$Id: test_tman.rb 662 2007-11-03 16:13:33Z toki $'

    STORAGE_ITEMS = (ENV['STORAGE_ITEMS'] || '100').to_i
    WARM_START_ITEMS = (ENV['WARM_START_ITEMS'] || '1000').to_i
    MAX_ITEM_BYTES = (ENV['MAX_ITEM_BYTES'] || '16384').to_i
    ITEM_CHARS = ('A'..'Z').to_a + ('a'..'z').to_a

    def setup
      srand(0)

      @name = 'foo'

      @src_dir = 'rep_src'
      @src_name = File.join(@src_dir, @name)
      FileUtils.rm_rf(@src_dir)
      FileUtils.mkdir_p(@src_dir)

      @dst_dir = 'rep_dst'
      @dst_name = File.join(@dst_dir, @name)
      FileUtils.rm_rf(@dst_dir)
      FileUtils.mkdir_p(@dst_dir)

      @jlog_apply_dir = 'rep_jlog'
      FileUtils.rm_rf(@jlog_apply_dir)
      FileUtils.mkdir_p(@jlog_apply_dir)

      @logger = proc{|path|
        logger = Logger.new(path, 1)
        logger.level = Logger::DEBUG
        logger
      }

      setup_storage
    end

    def teardown
      shutdown_storage
      FileUtils.rm_rf(@src_dir) unless $DEBUG
      FileUtils.rm_rf(@dst_dir) unless $DEBUG
      FileUtils.rm_rf(@jlog_apply_dir) unless $DEBUG
    end

    def move_jlog
      for jlog in Storage.rotated_entries("#{@src_name}.jlog")
        name = File.basename(jlog)
        target = File.join(@jlog_apply_dir, name)
        FileUtils.mv(jlog, target)
      end
      nil
    end
    private :move_jlog

    def test_replication_basic
      #### create ####

      @src.transaction{|tx|
        tx[:foo] = "Hello world.\n"
      }
      @dst.transaction{|tx|
        assert(tx.empty?)
      }

      rotate_journal_log
      move_jlog
      @dst.apply_journal_log

      @dst.transaction{|tx|
        assert(! tx.empty?)
        assert_equal([ :foo ], tx.keys)
        assert_equal("Hello world.\n", tx[:foo])
      }

      #### set property ####

      @src.transaction{|tx|
        tx.set_property(:foo, 'bar', 'Banana')
      }
      @dst.transaction{|tx|
        assert(! (tx.property? :foo, 'bar'))
      }

      rotate_journal_log
      move_jlog
      @dst.apply_journal_log

      @dst.transaction{|tx|
        assert((tx.property? :foo, 'bar'))
        assert_equal('Banana', tx.property(:foo, 'bar'))
      }

      #### update property ####

      @src.transaction{|tx|
        tx.set_property(:foo, 'bar', 'Orange')
      }
      @dst.transaction{|tx|
        assert((tx.property? :foo, 'bar'))
        assert_equal('Banana', tx.property(:foo, 'bar'))
      }

      rotate_journal_log
      move_jlog
      @dst.apply_journal_log

      @dst.transaction{|tx|
        assert((tx.property? :foo, 'bar'))
        assert_equal('Orange', tx.property(:foo, 'bar'))
      }

      #### delete property ####

      @src.transaction{|tx|
        tx.delete_property(:foo, 'bar')
      }
      @dst.transaction{|tx|
        assert((tx.property? :foo, 'bar'))
        assert_equal('Orange', tx.property(:foo, 'bar'))
      }

      rotate_journal_log
      move_jlog
      @dst.apply_journal_log

      @dst.transaction{|tx|
        assert(! (tx.property? :foo, 'bar'))
      }

      #### update ####

      @src.transaction{|tx|
        tx[:foo] = 'Apple'
      }
      @dst.transaction{|tx|
        assert(! tx.empty?)
        assert_equal([ :foo ], tx.keys)
        assert_equal("Hello world.\n", tx[:foo])
      }

      rotate_journal_log
      move_jlog
      @dst.apply_journal_log

      @dst.transaction{|tx|
        assert(! tx.empty?)
        assert_equal([ :foo ], tx.keys)
        assert_equal('Apple', tx[:foo])
      }

      #### delete ####

      @src.transaction{|tx|
        tx.delete(:foo)
      }
      @dst.transaction{|tx|
        assert(! tx.empty?)
        assert_equal([ :foo ], tx.keys)
        assert_equal('Apple', tx[:foo])
      }

      rotate_journal_log
      move_jlog
      @dst.apply_journal_log

      @dst.transaction{|tx|
        assert(tx.empty?)
      }

      ####

      shutdown_storage

      assert(FileUtils.cmp("#{@src_name}.tar", "#{@dst_name}.tar"), 'DATA should be same.')
      assert(Index.new.load("#{@src_name}.idx").to_h ==
             Index.new.load("#{@dst_name}.idx").to_h, 'INDEX should be same.')
    end

    def update_source_storage(options)
      count = 0
      operations = [
        :write_data,
        :delete_data,
        :write_system_properties,
        :write_custom_properties,
        :delete_custom_properties
      ]
      while (options[:spin_lock])
        count += 1
        options[:end_of_warm_up].start if (count == WARM_START_ITEMS)

        ope = operations[rand(operations.length)]
        key = rand(STORAGE_ITEMS)
        case (ope)
        when :write_data
          value = rand(256).chr * rand(MAX_ITEM_BYTES)
          @src.transaction{|tx|
            tx[key] = value
          }
        when :delete_data
          @src.transaction{|tx|
            tx.delete(key)
          }
        when :write_system_properties
          @src.transaction{|tx|
            if (tx.key? key) then
              tx.set_property(key, 'string_only', rand(2) > 0)
            end
          }
        when :write_custom_properties
          @src.transaction{|tx|
            if (tx.key? key) then
              value = ITEM_CHARS[rand(ITEM_CHARS.length)] * rand(MAX_ITEM_BYTES)
              tx.set_property(key, 'foo', value)
            end
          }
        when :delete_custom_properties
          @src.transaction{|tx|
            if (tx.key? key) then
              tx.delete_property(key, 'foo')
            end
          }
        else
          raise "unknown operation: #{ope}"
        end
      end
    end
    private :update_source_storage

    def test_update_source_storage
      options = {
        :end_of_warm_up => Latch.new,
        :spin_lock => true
      }
      t = Thread.new{ update_source_storage(options) }
      options[:end_of_warm_up].wait
      options[:spin_lock] = false
      t.join
    end

    def test_replication_with_multithread
      options = {
        :end_of_warm_up => Latch.new,
        :spin_lock => true
      }
      t = Thread.new{ update_source_storage(options) }
      options[:end_of_warm_up].wait

      10.times do
        sleep(0.01)
        move_jlog
        @dst.apply_journal_log
      end

      options[:spin_lock] = false
      t.join

      rotate_journal_log
      move_jlog
      @dst.apply_journal_log

      shutdown_storage

      assert(FileUtils.cmp("#{@src_name}.tar", "#{@dst_name}.tar"), 'DATA should be same.')
      assert(Index.new.load("#{@src_name}.idx").to_h ==
             Index.new.load("#{@dst_name}.idx").to_h, 'INDEX should be same.')
    end

    def test_switch_to_write
      # check standby mode
      assert_equal(:standby, @dst.read_only)
      @dst.transaction{|tx|
        assert_raise(NoMethodError) { tx[:foo] = "Hello world.\n" }
        assert_raise(NoMethodError) { tx.set_property(:foo, 'baz', 'orange') }
        assert_raise(NoMethodError) { tx.delete_property(:foo, 'bar') }
        assert_raise(NoMethodError) { tx.delete(:foo) }
        assert_raise(NoMethodError) { tx.delete_if{|key, value| value == 'apple' } }
        assert_raise(NoMethodError) { tx.clear }
        assert_raise(NoMethodError) { tx.commit }
        assert_raise(NoMethodError) { tx.rollback }
      }

      # replication enabled
      test_update_source_storage
      rotate_journal_log
      move_jlog
      @dst.apply_journal_log

      # standby -> read-write
      @dst.switch_to_write
      assert_equal(false, @dst.read_only)
      @dst.transaction{|tx|
        tx[:foo] = "Hello world.\n"
        tx.set_property(:foo, 'baz', 'orange')
        tx.delete_property(:foo, 'bar')
        tx.delete(:foo)
        tx.delete_if{|key, value| value == 'apple' }
        tx.clear
        tx.commit
        tx.rollback
      }

      # replication disabled
      test_update_source_storage
      rotate_journal_log
      move_jlog
      assert_raise(RuntimeError) {
        @dst.apply_journal_log
      }
    end

    def test_switch_to_write_RuntimeError_not_standby_mode
      assert_equal(false, @src.read_only)
      assert_raise(RuntimeError) {
        @src.switch_to_write
      }
    end
  end

  class TransactionManagerReplicationTest < Test::Unit::TestCase
    include Higgs
    include ReplicationTest

    # for ident(1)
    CVS_ID = '$Id: test_tman.rb 662 2007-11-03 16:13:33Z toki $'

    def setup_storage
      @src_st = Storage.new(@src_name,
                            :logger => @logger,
                            :jlog_rotate_max => 0)

      @src_st.rotate_journal_log(true)
      FileUtils.cp("#{@src_name}.tar", "#{@dst_name}.tar", :preserve => true)
      FileUtils.cp("#{@src_name}.idx", "#{@dst_name}.idx", :preserve => true)

      @dst_st = Storage.new(@dst_name,
                            :logger => @logger)

      for jlog_path in Storage.rotated_entries("#{@src_name}.jlog")
        @dst_st.apply_journal_log(jlog_path)
      end

      @src = TransactionManager.new(@src_st)
      @dst = TransactionManager.new(@dst_st,
                                    :read_only => :standby,
                                    :jlog_apply_dir => @jlog_apply_dir)
    end

    def shutdown_storage
      @src_st.shutdown if (@src_st && ! @src_st.shutdown?)
      @dst_st.shutdown if (@dst_st && ! @dst_st.shutdown?)
    end

    def rotate_journal_log
      @src_st.rotate_journal_log
    end
  end

  class StoreReplicationTest < Test::Unit::TestCase
    include Higgs
    include ReplicationTest

    # for ident(1)
    CVS_ID = '$Id$'

    def setup_storage
      @src = Store.new(@src_name,
                       :logger => @logger,
                       :jlog_rotate_max => 0)

      @src.rotate_journal_log(true)
      FileUtils.cp("#{@src_name}.tar", "#{@dst_name}.tar", :preserve => true)
      FileUtils.cp("#{@src_name}.idx", "#{@dst_name}.idx", :preserve => true)

      @dst = Store.new(@dst_name,
                       :logger => @logger,
                       :read_only => :standby,
                       :jlog_apply_dir => @jlog_apply_dir)

      for jlog_path in Storage.rotated_entries("#{@src_name}.jlog")
        @dst.apply_journal_log(jlog_path)
      end
    end

    def shutdown_storage
      @src.shutdown if (@src && ! @src.shutdown?)
      @dst.shutdown if (@dst && ! @dst.shutdown?)
    end

    def rotate_journal_log
      @src.rotate_journal_log
    end
  end

  class DBMReplicationTest < Test::Unit::TestCase
    include Higgs
    include ReplicationTest

    # for ident(1)
    CVS_ID = '$Id$'

    def setup_storage
      @src = DBM.new(@src_name,
                     :logger => @logger,
                     :jlog_rotate_max => 0)

      @src.rotate_journal_log(true)
      FileUtils.cp("#{@src_name}.tar", "#{@dst_name}.tar", :preserve => true)
      FileUtils.cp("#{@src_name}.idx", "#{@dst_name}.idx", :preserve => true)

      @dst = DBM.new(@dst_name,
                     :logger => @logger,
                     :read_only => :standby,
                     :jlog_apply_dir => @jlog_apply_dir)

      for jlog_path in Storage.rotated_entries("#{@src_name}.jlog")
        @dst.apply_journal_log(jlog_path)
      end
    end

    def shutdown_storage
      @src.shutdown if (@src && ! @src.shutdown?)
      @dst.shutdown if (@dst && ! @dst.shutdown?)
    end

    def rotate_journal_log
      @src.rotate_journal_log
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
