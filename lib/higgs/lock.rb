# $Id$

require 'forwardable'
require 'singleton'
require 'tank'
require 'tank/cache'
require 'thread'

module Tank
  module Lock
    # for ident(1)
    CVS_ID = '$Id$'

    class Error < StandardError
    end

    class TryLockTimeoutError < Error
    end

    class ReadWriteLock
      def initialize
        @lock = Mutex.new
        @r_cond = ConditionVariable.new
        @w_cond = ConditionVariable.new
        @read_count = 0
        @write_flag = false
      end

      def __read_lock__
        @lock.synchronize{
          while (@write_flag)
            @r_cond.wait(@lock)
          end
          @read_count += 1
        }
        nil
      end

      def __read_try_lock__
        @lock.synchronize{
          if (@write_flag) then
            return false
          else
            @read_count += 1
            return true
          end
        }
      end

      def __read_unlock__
        @lock.synchronize{
          @read_count -= 1
          if (@read_count == 0) then
            @w_cond.signal
          end
        }
        nil
      end

      def __write_lock__
        @lock.synchronize{
          while (@write_flag || @read_count > 0)
            @w_cond.wait(@lock)
          end
          @write_flag = true
        }
        nil
      end

      def __write_try_lock__
        @lock.synchronize{
          if (@write_flag || @read_count > 0) then
            return false
          else
            @write_flag = true
            return true
          end
        }
      end

      def __write_unlock__
        @lock.synchronize{
          @write_flag = false
          @w_cond.signal
          @r_cond.broadcast
        }
        nil
      end

      class ChildLock
        def initialize(rw_lock)
          @rw_lock = rw_lock
        end

        def synchronize
          lock
          begin
            r = yield
          ensure
            unlock
          end
          r
        end
      end

      class ReadLock < ChildLock
        extend Forwardable

        def_delegator :@rw_lock, :__read_lock__,     :lock
        def_delegator :@rw_lock, :__read_try_lock__, :try_lock
        def_delegator :@rw_lock, :__read_unlock__,   :unlock
      end

      class WriteLock < ChildLock
        extend Forwardable

        def_delegator :@rw_lock, :__write_lock__,     :lock
        def_delegator :@rw_lock, :__write_try_lock__, :try_lock
        def_delegator :@rw_lock, :__write_unlock__,   :unlock
      end

      def read_lock
        ReadLock.new(self)
      end

      def write_lock
        WriteLock.new(self)
      end
    end

    class LockManager
      def initialize(options={})
        @spin_lock_count = options[:spin_lock_count] || 1000
        @try_lock_limit = options[:try_lock_limit] || 10
        @try_lock_interval = options[:try_lock_interval] || 0.1
      end

      attr_reader :spin_lock_count
      attr_reader :try_lock_limit
      attr_reader :try_lock_interval

      def self.try_lock(lock, attrs)
        t0 = Time.now
        c = attrs.spin_lock_count
        while (c > 0)
          if (lock.try_lock) then
            return
          end
          c -= 1
        end
        while (Time.now - t0 < attrs.try_lock_limit)
          if (lock.try_lock) then
            return
          end
          sleep(attrs.try_lock_interval)
        end
        raise TryLockTimeoutError, 'expired'
      end
    end

    class GiantLockManager < LockManager
      def initialize(*args)
        super
        @rw_lock = ReadWriteLock.new
      end

      class NoWorkLockHandler
        include Singleton

        def lock(key)
          self
        end
      end

      def transaction(read_only=false)
        if (read_only) then
          lock = @rw_lock.read_lock
        else
          lock = @rw_lock.write_lock
        end
        LockManager.try_lock(lock, self)
        begin
          yield(NoWorkLockHandler.instance)
        ensure
          lock.unlock
        end
      end
    end

    class FineGrainLockManager < LockManager
      def initialize(*args)
        super
        @cache = Cache::SharedWorkCache.new{|key| ReadWriteLock.new }
      end

      class LockHandler
        def initialize(attrs, cache)
          @attrs = attrs
          @cache = cache
          @lock_list = []
        end

        attr_reader :lock_list
      end

      class ReadOnlyLockHandler < LockHandler
        def lock(key)
          r_lock = @cache[key].read_lock
          LockManager.try_lock(r_lock, @attrs)
          @lock_list << r_lock
          self
        end
      end

      class ReadWriteLockHandler < LockHandler
        def lock(key)
          w_lock = @cache[key].write_lock
          LockManager.try_lock(w_lock, @attrs)
          @lock_list << w_lock
          self
        end
      end

      def transaction(read_only=false)
        if (read_only) then
          lock_handler = ReadOnlyLockHandler.new(self, @cache)
        else
          lock_handler = ReadWriteLockHandler.new(self, @cache)
        end
        begin
          r = yield(lock_handler)
        ensure
          for lock in lock_handler.lock_list
            lock.unlock
          end
        end
        r
      end
    end
  end
end
