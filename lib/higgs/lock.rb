# $Id$

require 'forwardable'
require 'higgs/cache'
require 'singleton'
require 'thread'

module Higgs
  module Lock
    # for ident(1)
    CVS_ID = '$Id$'

    class Error < StandardError
    end

    class TryLockTimeoutError < Error
    end

    class LockManager
      RAND_GEN = proc{|seed|
        n = seed
        cycle = 0xFFFF
        proc{
          n = (n * 37549 + 12345) % cycle
          n.to_f / cycle
        }
      }

      def initialize(options={})
        @spin_lock_count = options[:spin_lock_count] || 1000
        @try_lock_limit = options[:try_lock_limit] || 10
        @try_lock_interval = options[:try_lock_interval] || 0.1
        @rand_gen = options[:random_number_generator] || RAND_GEN
      end

      attr_reader :spin_lock_count
      attr_reader :try_lock_limit
      attr_reader :try_lock_interval

      def new_rand(seed)
        @rand_gen.call(seed)
      end

      def self.try_lock(lock, attrs)
        t0 = Time.now
        c = attrs.spin_lock_count
        while (c > 0)
          if (lock.try_lock) then
            return
          end
          c -= 1
        end
        rand = attrs.new_rand(::Thread.current.object_id ^ t0.to_i)
        while (Time.now - t0 < attrs.try_lock_limit)
          if (lock.try_lock) then
            return
          end
          sleep(attrs.try_lock_interval * rand.call)
        end
        raise TryLockTimeoutError, 'expired'
      end
    end

    class GiantLockManager < LockManager
      def initialize(*args)
        super
        @rw_lock = Thread::ReadWriteLock.new
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
        @cache = Cache::SharedWorkCache.new{|key| Thread::ReadWriteLock.new }
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
