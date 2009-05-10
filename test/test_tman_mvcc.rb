#!/usr/local/bin/ruby

require 'fileutils'
require 'higgs/storage'
require 'higgs/thread'
require 'higgs/tman'
require 'logger'
require 'test/unit'
require 'timeout'

Thread.abort_on_exception = true if $DEBUG

module Higgs::Test
  class TransactionManagerMVCCTest < Test::Unit::TestCase
    include Higgs
    include Timeout

    # for ident(1)
    CVS_ID = '$Id$'

    def create_lock_manager
      GiantLockManager.new
    end

    def setup
      @test_dir = 'st_test'
      FileUtils.rm_rf(@test_dir) # for debug
      FileUtils.mkdir_p(@test_dir)
      @st_name = File.join(@test_dir, 'foo')
      @logger = proc{|path|
        logger = Logger.new(path, 1)
        logger.level = Logger::DEBUG
        logger
      }
      @st = Storage.new(@st_name, :logger => @logger)
      @tman = TransactionManager.new(@st, :lock_manager => create_lock_manager)
    end

    def teardown
      @st.shutdown unless @st.shutdown?
      FileUtils.rm_rf(@test_dir) unless $DEBUG
    end

    MVCC_WARMUP_COUNT = 10
    WRITE_TIMEOUT = 10

    class RunFlag
      def initialize(running)
        @lock = Mutex.new
        @running = running
      end

      def running=(running)
        @lock.synchronize{
          @running = running
        }
      end

      def running?
        @lock.synchronize{
          @running
        }
      end
    end

    class Count
      def initialize
        @value = 0
        @lock = Mutex.new
      end

      def succ!
        @lock.synchronize{ @value += 1 }
      end

      def value
        @lock.synchronize{ @value }
      end
    end

    def test_mvcc
      do_read = RunFlag.new(true)

      init_read_latch = Latch.new
      init_read_count = Count.new
      init_read_thread = Thread.new{
        @tman.transaction(true) {|tx|
          while (do_read.running?)
            assert_equal([], tx.keys)
            assert_nil(tx[:foo])
            init_read_count.succ!
            init_read_latch.start if (init_read_count.value == MVCC_WARMUP_COUNT)
          end
        }
      }
      init_read_latch.wait

      # insert
      p [ self.class, :test_mvcc, :insert ] if $DEBUG
      timeout(WRITE_TIMEOUT) {
        @tman.transaction{|tx|
          tx[:foo] = 'Hello world.'
        }
      }

      insert_read_latch = Latch.new
      insert_read_count = Count.new
      insert_read_thread = Thread.new{
        @tman.transaction(true) {|tx|
          while (do_read.running?)
            assert_equal([ :foo ], tx.keys)
            assert_equal('Hello world.', tx[:foo])
            insert_read_count.succ!
            insert_read_latch.start if (insert_read_count.value == MVCC_WARMUP_COUNT)
          end
        }
      }
      insert_read_latch.wait

      # update
      p [ self.class, :test_mvcc, :update ] if $DEBUG
      timeout(WRITE_TIMEOUT) {
        @tman.transaction{|tx|
          tx[:foo] = 'I like ruby.'
        }
      }

      update_read_latch = Latch.new
      update_read_count = Count.new
      update_read_thread = Thread.new{
        @tman.transaction(true) {|tx|
          while (do_read.running?)
            assert_equal([ :foo ], tx.keys)
            assert_equal('I like ruby.', tx[:foo])
            update_read_count.succ!
            update_read_latch.start if (update_read_count.value == MVCC_WARMUP_COUNT)
          end
        }
      }
      update_read_latch.wait

      # delete
      p [ self.class, :test_mvcc, :delete ] if $DEBUG
      timeout(WRITE_TIMEOUT) {
        @tman.transaction{|tx|
          tx.delete(:foo)
        }
      }

      delete_read_latch = Latch.new
      delete_read_count = Count.new
      delete_read_thread = Thread.new{
        @tman.transaction(true) {|tx|
          count = 0
          while (do_read.running?)
            assert_equal([], tx.keys)
            assert_nil(tx[:foo])
            delete_read_count.succ!
            delete_read_latch.start if (delete_read_count.value == MVCC_WARMUP_COUNT)
          end
        }
      }
      delete_read_latch.wait

      # insert
      p [ self.class, :test_mvcc, :insert2 ] if $DEBUG
      timeout(WRITE_TIMEOUT) {
        @tman.transaction{|tx|
          tx[:foo] = 'Hello world.'
        }
      }

      [ init_read_count,
        insert_read_count,
        update_read_count,
        delete_read_count
      ].each do |count|
        c = count.value
        until (count.value > c)
          # nothing to do.
        end
      end

      do_read.running = false
      [ init_read_thread,
        update_read_thread,
        update_read_thread,
        delete_read_thread
      ].each do |thread|
        thread.join
      end

      @tman.transaction(true) {|tx|
        assert_equal([ :foo ], tx.keys)
        assert_equal('Hello world.', tx[:foo])
      }
    end

    WORK_COUNT = 100
    READ_THREAD_COUNT = 10
    WRITE_THREAD_COUNT = 2

    def test_read_write_multithread_mvcc
      do_read = RunFlag.new(true)
      th_read_list = []
      th_write_list = []
      barrier = Barrier.new((READ_THREAD_COUNT + WRITE_THREAD_COUNT) + 1)

      READ_THREAD_COUNT.times{|i|
        th_read_list << Thread.new{
          barrier.wait
          while (do_read.running?)
            @tman.transaction(true) {|tx|
              if (tx.key? :foo) then
                assert_equal(tx.change_number.to_s, tx[:foo], "thread: #{i}")
              end
            }
          end
        }
      }

      WRITE_THREAD_COUNT.times{|i|
        th_write_list << Thread.new{
          barrier.wait
          WORK_COUNT.times do |j|
            begin
              @tman.transaction{|tx|
                value = tx[:foo] || '0'
                assert_equal(tx.change_number.to_s, value, "thread-count: #{i}-#{j}")
                tx[:foo] = value.succ
              }
            rescue LockManager::CollisionError
              retry
            end
          end
        }
      }

      barrier.wait
      for t in th_write_list
        t.join
      end

      do_read.running = false
      for t in th_read_list
        t.join
      end

      @tman.transaction(true) {|tx|
        assert_equal((WRITE_THREAD_COUNT * WORK_COUNT).to_s, tx[:foo])
      }
    end
  end

  class TransactionManagerMVCCTest_with_OptimisticLockManager < TransactionManagerMVCCTest
    def create_lock_manager
      OptimisticLockManager.new
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
