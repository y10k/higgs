# $Id$

require 'singleton'
require 'sync'
require 'tank'
require 'tank/cache'

module Tank
  class LockError < Error
    # for ident(1)
    CVS_ID = '$Id$'
  end

  class TryLockTimeoutError < LockError
    # for ident(1)
    CVS_ID = '$Id$'
  end

  class LockManager
    # for ident(1)
    CVS_ID = '$Id$'

    def initialize(options={})
      @spin_lock_count = options[:spin_lock_count] || 1000
      @try_lock_limit = options[:try_lock_limit] || 10
      @try_lock_interval = options[:try_lock_interval] || 0.1
    end

    attr_reader :spin_lock_count
    attr_reader :try_lock_limit
    attr_reader :try_lock_interval

    def self.try_lock(sync, mode, attrs)
      t0 = Time.now
      c = attrs.spin_lock_count
      while (c > 0)
        if (sync.try_lock(mode)) then
          return
        end
        c -= 1
      end
      while (Time.now - t0 < attrs.try_lock_limit)
        if (sync.try_lock(mode)) then
          return
        end
        sleep(attrs.try_lock_interval)
      end
      raise TryLockTimeoutError, 'expired'
    end
  end

  class GiantLockManager < LockManager
    # for ident(1)
    CVS_ID = '$Id$'

    def initialize(*args)
      super
      @sync = Sync.new
    end

    class NoWorkLockHandler
      include Singleton

      def lock(key)
        self
      end
    end

    def transaction(read_only)
      if (read_only) then
        mode = Sync::SH
      else
        mode = Sync::EX
      end
      LockManager.try_lock(@sync, mode, self)
      begin
        yield(NoWorkLockHandler.instance)
      ensure
        @sync.unlock
      end
    end
  end

  class FineGrainLockManager < LockManager
    # for ident(1)
    CVS_ID = '$Id$'

    def initialize(*args)
      super
      @cache = Cache::SharedWorkCache.new{|key| Sync.new }
    end

    class LockHandler
      def initialize(attrs, cache)
        @attrs = attrs
        @cache = cache
        @locked_keys = []
      end

      attr_reader :locked_keys
    end

    class ReadOnlyLockHandler < LockHandler
      def lock(key)
        sync = @cache[key]
        LockManager.try_lock(sync, Sync::SH, @attrs)
        @locked_keys << key
        self
      end
    end

    class ReadWriteLockHandler < LockHandler
      def lock(key)
        sync = @cache[key]
        LockManager.try_lock(sync, Sync::EX, @attrs)
        @locked_keys << key
        self
      end
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
