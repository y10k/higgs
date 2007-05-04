#!/usr/local/bin/ruby

require 'higgs/lock'
require 'higgs/thread'
require 'test/unit'
require 'thwait'

module Higgs::LockTest
  # for ident(1)
  CVS_ID = '$Id$'

  module LockManagerTest
    include Higgs::Lock
    include Higgs::Thread

    WORK_COUNT = 100
    THREAD_COUNT = 10

    def test_read_transaction_single_thread
      v = "foo"
      WORK_COUNT.times do
        @lock_manager.transaction(true) {|lock_handler|
          lock_handler.lock(:foo)
          assert_equal("foo", v)
        }
      end
    end

    def test_write_transaction_single_thread
      count = 0
      WORK_COUNT.times do
        @lock_manager.transaction{|lock_handler|
          lock_handler.lock(:foo)
          count += 1
        }
      end
      assert_equal(WORK_COUNT, count)
    end

    def test_read_transaction_multithread
      v = "foo"
      th_grp = ThreadGroup.new
      barrier = Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times{|i|
        th_grp.add Thread.new{
          @lock_manager.transaction(true) {|lock_handler|
            lock_handler.lock(:foo)
            barrier.wait
            WORK_COUNT.times do |j|
              assert_equal("foo", v, "read transaction: #{i}.#{j}")
            end
          }
        }
      }

      barrier.wait
      for t in th_grp.list
        t.join
      end
    end

    def test_write_transaction_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times do
        th_grp.add Thread.new{
          barrier.wait
          WORK_COUNT.times do
            @lock_manager.transaction{|lock_handler|
              lock_handler.lock(:foo)
              count += 1
            }
          end
        }
      end

      barrier.wait
      for t in th_grp.list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
    end

    def test_read_write_transaction_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Barrier.new(THREAD_COUNT * 2 + 1)

      THREAD_COUNT.times{|i|
        th_grp.add Thread.new{
          @lock_manager.transaction(true) {|lock_handler|
            lock_handler.lock(:foo)
            barrier.wait
            WORK_COUNT.times do |j|
              assert_equal(0, count, "read transaction: #{i}.#{j}")
            end
          }
        }
      }

      THREAD_COUNT.times do
        th_grp.add Thread.new{
          barrier.wait
          WORK_COUNT.times do
            @lock_manager.transaction{|lock_handler|
              lock_handler.lock(:foo)
              count += 1
            }
          end
        }
      end

      barrier.wait
      for t in th_grp.list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
    end
  end

  class GiantLockManagerTest < Test::Unit::TestCase
    include Higgs::Lock
    include Higgs::Thread
    include LockManagerTest

    def setup
      @lock_manager = GiantLockManager.new
    end

    def test_write_read_transaction_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Barrier.new(THREAD_COUNT + 2)

      th_grp.add Thread.new{
        @lock_manager.transaction{|lock_handler|
          lock_handler.lock(:foo)
          barrier.wait
          THREAD_COUNT.times do
            WORK_COUNT.times do
              count += 1
            end
          end
        }
      }

      THREAD_COUNT.times{|i|
        th_grp.add Thread.new{
          barrier.wait          # different point from FineGrainLockManager
          @lock_manager.transaction(true) {|lock_handler|
            lock_handler.lock(:foo)
            assert_equal(THREAD_COUNT * WORK_COUNT, count, "read transaction: #{i}")
          }
        }
      }

      barrier.wait
      for t in th_grp.list
        t.join
      end
    end
  end

  class FineGrainLockManagerTest < Test::Unit::TestCase
    include Higgs::Lock
    include Higgs::Thread
    include LockManagerTest

    def setup
      @lock_manager = FineGrainLockManager.new
    end

    def test_write_read_transaction_multithread
      count = 0
      th_grp = ThreadGroup.new
      barrier = Barrier.new(THREAD_COUNT + 2)

      th_grp.add Thread.new{
        @lock_manager.transaction{|lock_handler|
          lock_handler.lock(:foo)
          barrier.wait
          THREAD_COUNT.times do
            WORK_COUNT.times do
              count += 1
            end
          end
        }
      }

      THREAD_COUNT.times{|i|
        th_grp.add Thread.new{
          @lock_manager.transaction(true) {|lock_handler|
            barrier.wait   # different point from GiantLockManager
            lock_handler.lock(:foo)
            assert_equal(THREAD_COUNT * WORK_COUNT, count, "read transaction: #{i}")
          }
        }
      }

      barrier.wait
      for t in th_grp.list
        t.join
      end
    end
  end

  class GiantLockManagerNoDeadLockTest < Test::Unit::TestCase
    include Higgs::Lock
    include Higgs::Thread

    WORK_COUNT = 1000

    def setup
      @lock_manager = GiantLockManager.new
    end

    def test_transaction_no_dead_lock
      barrier = Barrier.new(3)

      t1 = Thread.new{
        barrier.wait
        WORK_COUNT.times do
          @lock_manager.transaction{|lock_handler|
            lock_handler.lock(:foo)
            lock_handler.lock(:bar)
          }
        end
      }

      t2 = Thread.new{
        barrier.wait
        WORK_COUNT.times do
          @lock_manager.transaction{|lock_handler|
            lock_handler.lock(:bar)
            lock_handler.lock(:foo)
          }
        end
      }

      barrier.wait
      t1.join
      t2.join
    end
  end

  class FineGrainLockManagerDeadLockTest < Test::Unit::TestCase
    include Higgs::Lock
    include Higgs::Thread

    def setup
      @lock_manager = FineGrainLockManager.new(:spin_lock_count   => 10,
                                               :try_lock_limit    => 0.1,
                                               :try_lock_interval => 0.001)
    end

    def test_transaction_dead_lock
      barrier = Barrier.new(3)

      m1 = Mutex.new
      end_of_t1 = false

      m2 = Mutex.new
      end_of_t2 = false

      t1 = Thread.new{
        barrier.wait
        begin
          until (m2.synchronize{ end_of_t2 })
            @lock_manager.transaction{|lock_handler|
              lock_handler.lock(:foo)
              sleep(0.03)
              lock_handler.lock(:bar)
            }
          end
        ensure
          m1.synchronize{ end_of_t1 = true }
        end
      }

      t2 = Thread.new{
        barrier.wait
        begin
          until (m1.synchronize{ end_of_t1 })
            @lock_manager.transaction{|lock_handler|
              lock_handler.lock(:bar)
              sleep(0.07)
              lock_handler.lock(:foo)
            }
          end
        ensure
          m2.synchronize{ end_of_t2 = true }
        end
      }

      barrier.wait
      assert_raise(TryLockTimeoutError) {
        t1.join
        t2.join
      }

      # join threads without exception
      ThreadsWait.all_waits(t1, t2)
    end
  end
end
