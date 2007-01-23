# $Id$

require 'sync'
require 'tank'
require 'tank/cache'

module Tank
  class LockError < Error
  end

  class TryLockTimeoutError < LockError
  end

  class LockManager
    def initialize(options={})
      @spin_lock_limit = options[:spin_lock_limit] || 0.01
      @try_lock_limit = options[:try_lock_limit] || 10
      @try_lock_interval = options[:try_lock_interval] || 0.1
    end

    attr_reader :spin_lock_limit
    attr_reader :try_lock_limit
    attr_reader :try_lock_interval

    def self.try_lock(sync, mode, attrs)
      t0 = Time.now
      loop do
        if (sync.try_lock(mode)) then
          return
        end
        if (Time.now - t0 > attrs.spin_lock_limit) then
          break
        end
      end
      loop do
        if (sync.try_lock(mode)) then
          return
        end
        sleep(attrs.try_lock_interval)
        if (Time.now - t0 > attrs.try_lock_limit) then
          break
        end
      end
      raise TryLockTimeoutError, 'expired'
    end
  end

  class GiantLockManager < LockManager
    def initialize(*args)
      super
      @sync = Sync.new
    end

    class ReadOnlyLockHandler
      def read_only_lock(key)
      end
    end

    class ReadWriteLockHandler
      def read_only_lock(key)
      end

      def read_write_lock(key)
      end
    end

    def transaction(read_only)
      if (read_only) then
        mode = Sync::SH
        handler = ReadOnlyLockHandler.new
      else
        mode = Sync::EX
        handler = ReadWriteLockHandler.new
      end
      LockManager.try_lock(@sync, mode, self)
      begin
        yield(handler)
      ensure
        @sync.unlock
      end
    end
  end

  class FineGrainLockManager < LockManager
    def initialize(*args)
      super
      @cache = SharedWorkCache.new{|key| Sync.new }
    end

    class LockHandler
      def initialize(attrs, cache)
        @attrs = attrs
        @cache = cache
        @locked_keys = []
      end

      attr_reader :locked_keys
    end

    module ReadOnlyLock
      def read_only_lock(key)
        sync = @cache[key]
        LockManager.try_lock(sync, Sync::SH, @attrs)
        @locked_keys << key
        self
      end
    end

    module ReadWriteLock
      def read_write_lock(key)
        sync = @cache[key]
        LockManager.try_lock(sync, Sync::EX, @attrs)
        @locked_keys << key
        self
      end
    end

    class ReadOnlyLockHandler < LockHandler
      include ReadOnlyLock
    end

    class ReadWriteLockHandler < LockHandler
      include ReadOnlyLock
      include ReadWriteLock
    end

    def transaction(read_only=false)
      if (read_only) then
        handler = ReadOnlyLockHandler.new(self, @cache)
      else
        handler = ReadWriteLockHandler.new(self, @cache)
      end
      begin
        yield(handler)
      ensure
        for key in handler.locked_keys
          sync = @cache[key]
          sync.unlock
        end
      end
    end
  end
end
