#!/usr/local/bin/ruby

require 'higgs/lock'
require 'higgs/thread'
require 'test/unit'

Thread.abort_on_exception = true if $DEBUG

module Higgs::Test
  module LockManagerTest
    include Higgs

    # for ident(1)
    CVS_ID = '$Id$'

    WORK_COUNT = 1000
    THREAD_COUNT = 10

    def test_read_transaction_single_thread
      v = "foo"
      WORK_COUNT.times do
        @lock_manager.transaction(true) {|lock_handler|
          lock_handler.lock(:foo, :data, 0)
          assert_equal("foo", v)
        }
      end
    end

    def test_write_transaction_single_thread
      count = 0
      WORK_COUNT.times do
        @lock_manager.transaction{|lock_handler|
          lock_handler.lock(:foo, :data, 0)
          check_collision(lock_handler, [ :foo, :data ] => 0)
          count += 1
        }
      end
      assert_equal(WORK_COUNT, count)
    end

    def test_read_transaction_multithread
      v = "foo"
      th_list = []
      barrier = Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times{|i|
        th_list << Thread.new{
          @lock_manager.transaction(true) {|lock_handler|
            lock_handler.lock(:foo, :data, 0)
            barrier.wait
            WORK_COUNT.times do |j|
              assert_equal("foo", v, "read transaction: #{i}-#{j}")
            end
          }
        }
      }

      barrier.wait
      for t in th_list
        t.join
      end
    end

    def test_write_transaction_multithread
      count = 0
      mvcc_last_cnum = 0
      mvcc_old_values = {}
      mvcc_lock = Mutex.new
      th_list = []
      barrier = Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times do
        th_list << Thread.new{
          barrier.wait
          WORK_COUNT.times do
            begin
              @lock_manager.transaction{|lock_handler|
                curr_cnum = mvcc_last_cnum # save at first
                lock_handler.lock(:foo, :data, curr_cnum)
                next_count = mvcc_lock.synchronize{ mvcc_old_values[curr_cnum] || count }
                next_count += 1
                lock_handler.critical{
                  check_collision(lock_handler, [ :foo, :data ] => mvcc_last_cnum)
                  mvcc_lock.synchronize{
                    mvcc_old_values[mvcc_last_cnum] = count
                  }
                  count = next_count
                  mvcc_last_cnum += 1 # increment at last
                }
              }
            rescue LockManager::CollisionError
              retry
            end
          end
        }
      end

      barrier.wait
      for t in th_list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
    end

    def test_read_write_transaction_multithread
      count = 0
      mvcc_last_cnum = 0
      mvcc_old_values = {}
      mvcc_lock = Mutex.new
      th_list = []
      barrier = Barrier.new(THREAD_COUNT * 2 + 1)

      THREAD_COUNT.times{|i|
        th_list << Thread.new{
          @lock_manager.transaction(true) {|lock_handler|
            curr_cnum = mvcc_last_cnum # save at first
            lock_handler.lock(:foo, :data, curr_cnum)
            barrier.wait
            WORK_COUNT.times do |j|
              assert_equal(curr_cnum,
                           mvcc_lock.synchronize { mvcc_old_values[curr_cnum] || count },
                           "read transaction: #{i}-#{j}")
            end
          }
        }
      }

      THREAD_COUNT.times do
        th_list << Thread.new{
          barrier.wait
          WORK_COUNT.times do
            begin
              @lock_manager.transaction{|lock_handler|
                curr_cnum = mvcc_last_cnum # save at first
                lock_handler.lock(:foo, :data, curr_cnum)
                next_count = mvcc_lock.synchronize{
                  mvcc_old_values[curr_cnum] || count
                }
                next_count += 1
                lock_handler.critical{
                  check_collision(lock_handler, [ :foo, :data ] => mvcc_last_cnum)
                  mvcc_lock.synchronize{
                    mvcc_old_values[mvcc_last_cnum] = count
                  }
                  count = next_count
                  mvcc_last_cnum += 1 # increment at last
                }
              }
            rescue LockManager::CollisionError
              retry
            end
          end
        }
      end

      barrier.wait
      for t in th_list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
    end

    def test_exclusive_single_thread
      v = "foo"
      WORK_COUNT.times do
        @lock_manager.exclusive{
          assert_equal("foo", v)
        }
      end
    end

    def test_exclusive_multithread
      count = 0
      th_list = []
      barrier = Barrier.new(THREAD_COUNT + 1)

      THREAD_COUNT.times do
        th_list << Thread.new{
          barrier.wait
          WORK_COUNT.times do
            @lock_manager.exclusive{
              count += 1
            }
          end
        }
      end

      barrier.wait
      for t in th_list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
    end

    def test_read_write_exclusive_multithread
      state = :transaction
      count = 0
      ex_count = 0
      mvcc_last_cnum = 0
      mvcc_old_values = {}
      mvcc_lock = Mutex.new
      th_list = []
      barrier = Barrier.new(THREAD_COUNT * 3 + 1)

      THREAD_COUNT.times{|i|
        th_list << Thread.new{
          barrier.wait
          WORK_COUNT.times do |j|
            @lock_manager.transaction(true) {|lock_handler|
              assert_equal(:transaction, state)
              curr_cnum = mvcc_last_cnum # save at first
              lock_handler.lock(:foo, :data, curr_cnum)
              assert_equal(curr_cnum,
                           mvcc_lock.synchronize{ mvcc_old_values[curr_cnum] || count },
                           "read transaction: #{i}-#{j}")
              assert_equal(:transaction, state)
            }
          end
        }
      }

      THREAD_COUNT.times{|i|
        th_list << Thread.new{
          barrier.wait
          WORK_COUNT.times do |j|
            begin
              @lock_manager.transaction{|lock_handler|
                assert_equal(:transaction, state)
                curr_cnum = mvcc_last_cnum # save at first
                lock_handler.lock(:foo, :data, curr_cnum)
                next_count = mvcc_lock.synchronize{ mvcc_old_values[curr_cnum] || count }
                next_count += 1
                lock_handler.critical{
                  check_collision(lock_handler, [ :foo, :data ] => mvcc_last_cnum)
                  mvcc_lock.synchronize{
                    mvcc_old_values[mvcc_last_cnum] = count
                  }
                  count = next_count
                  ex_count += 1
                  mvcc_last_cnum += 1 # increment at last
                }
                assert_equal(:transaction, state)
              }
            rescue LockManager::CollisionError
              retry
            end
          end
        }
      }

      THREAD_COUNT.times{|i|
        th_list << Thread.new{
          barrier.wait
          WORK_COUNT.times do |j|
            @lock_manager.exclusive{
              save_state = state
              state = :exclusive
              ex_count += 1
              state = save_state
            }
          end
        }
      }

      barrier.wait
      for t in th_list
        t.join
      end
      assert_equal(THREAD_COUNT * WORK_COUNT, count)
      assert_equal(THREAD_COUNT * WORK_COUNT * 2, ex_count)
    end

    def test_transaction_no_dead_lock
      barrier = Barrier.new(3)

      t1 = Thread.new{
        barrier.wait
        WORK_COUNT.times do
          @lock_manager.transaction{|lock_handler|
            lock_handler.lock(:foo, :data, 0)
            lock_handler.lock(:bar, :data, 0)
            check_collision(lock_handler, [ :foo, :data ] => 0, [ :bar, :data ] => 0)
          }
        end
      }

      t2 = Thread.new{
        barrier.wait
        WORK_COUNT.times do
          @lock_manager.transaction{|lock_handler|
            lock_handler.lock(:bar, :data, 1)
            lock_handler.lock(:foo, :data, 1)
            check_collision(lock_handler, [ :foo, :data ] => 1, [ :bar, :data ] => 1)
          }
        end
      }

      barrier.wait
      t1.join
      t2.join
    end
  end

  class GiantLockManagerTest < Test::Unit::TestCase
    include Higgs
    include LockManagerTest

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @lock_manager = GiantLockManager.new
    end

    def check_collision(lock_handler, expected_cnum)
      lock_handler.check_collision{|key, type|
        flunk("not to reach.")
      }
    end
  end

  class OptimisticLockManagerTest < Test::Unit::TestCase
    include Higgs
    include LockManagerTest

    # for ident(1)
    CVS_ID = '$Id$'

    def setup
      @lock_manager = OptimisticLockManager.new
    end

    def check_collision(lock_handler, expected_cnum)
      lock_handler.check_collision{|key, type|
        key_pair = [ key, type ]
        expected_cnum.delete(key_pair) or flunk("not defined key: `#{key}(#{type})'")
      }
      assert_equal({}, expected_cnum)
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
