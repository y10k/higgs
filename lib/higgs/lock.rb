# $Id$

require 'forwardable'
require 'higgs/cache'
require 'higgs/exceptions'
require 'singleton'
require 'thread'

module Higgs
  class LockManager
    # for ident(1)
    CVS_ID = '$Id$'

    include Exceptions

    class Error < HiggsError
    end

    class TryLockTimeoutError < Error
    end

    SPIN_LOCK_COUNT = 100
    TRY_LOCK_LIMIT = 10
    TRY_LOCK_INTERVAL = 0.1

    RAND_GEN = proc{|seed|
      n = seed
      cycle = 0xFFFF
      proc{
        n = (n * 37549 + 12345) % cycle
        n.to_f / cycle
      }
    }

    def initialize(options={})
      @spin_lock_count = options[:spin_lock_count] || SPIN_LOCK_COUNT
      @try_lock_limit = options[:try_lock_limit] || TRY_LOCK_LIMIT
      @try_lock_interval = options[:try_lock_interval] || TRY_LOCK_INTERVAL
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

      if (attrs.try_lock_limit > 0) then
        rand = attrs.new_rand(::Thread.current.object_id ^ t0.to_i)
        while (Time.now - t0 < attrs.try_lock_limit)
          if (lock.try_lock) then
            return
          end
          sleep(attrs.try_lock_interval * rand.call)
        end
        raise TryLockTimeoutError, 'expired'
      else
        # brave man who doesn't fear deadlock.
        lock.lock
      end
    end
  end

  class GiantLockManager < LockManager
    # for ident(1)
    CVS_ID = '$Id$'

    def initialize(*args)
      super
      @rw_lock = ReadWriteLock.new
    end

    class NoWorkLockHandler
      include Singleton

      def lock(key)
        self
      end

      def unlock(key)
        self
      end
    end

    def transaction(read_only=false)
      if (read_only) then
        lock = @rw_lock.read_lock
      else
        lock = @rw_lock.write_lock
      end
      r = nil
      lock.synchronize{
        r = yield(NoWorkLockHandler.instance)
      }
      r
    end
  end

  class FineGrainLockManager < LockManager
    # for ident(1)
    CVS_ID = '$Id$'

    def initialize(*args)
      super
      @cache = SharedWorkCache.new{|key| ReadWriteLock.new }
    end

    class LockHandler
      def initialize(attrs, cache)
        @attrs = attrs
        @cache = cache
        @lock_map = {}
      end

      def lock_list
        @lock_map.values
      end

      def unlock(key)
        if (lock = @lock_map.delete(key)) then
          lock.unlock
        else
          raise "not locked key: #{key}"
        end
        self
      end
    end

    class ReadOnlyLockHandler < LockHandler
      def lock(key)
        r_lock = @cache[key].read_lock
        LockManager.try_lock(r_lock, @attrs)
        @lock_map[key] = r_lock
        self
      end
    end

    class ReadWriteLockHandler < LockHandler
      def lock(key)
        w_lock = @cache[key].write_lock
        LockManager.try_lock(w_lock, @attrs)
        @lock_map[key] = w_lock
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

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
