# -*- coding: utf-8 -*-
# = multi-thread lock manager
# == license
#   :include:../../LICENSE
#

require 'higgs/cache'
require 'higgs/exceptions'
require 'singleton'
require 'thread'

module Higgs
  # = multi-thread lock manager
  class LockManager
    include Exceptions

    class Error < HiggsError
    end

    class CollisionError < Error
    end

    class NoWorkLockHandler
      include Singleton

      def lock(key, type, cnum)
        self
      end

      def check_collision       # :yields: key
        self
      end

      def critical
        yield
      end
    end

    class CriticalRegionLockHandler
      def initialize(critical_lock)
        @critical_lock = critical_lock
      end

      def lock(key, type, cnum)
        self
      end

      def check_collision       # :yields: key
        self
      end

      def critical
        @critical_lock.synchronize{ yield }
      end
    end

    class CollisionCheckLockHandler < CriticalRegionLockHandler
      def initialize(*args)
        super
        @cnum_map = {}
      end

      def lock(key, type, cnum)
        key_pair = [ key, type ]
        if (@cnum_map[key_pair] && @cnum_map[key_pair] != cnum) then
          raise "unexpected changed cnum at`#{key}(#{type})': #{@cnum_map[key].inspect} -> #{cnum.inspect}"
        end
        @cnum_map[key_pair] = cnum
        self
      end

      def check_collision
        for (key, type), cnum in @cnum_map
          last_cnum = yield(key, type)
          if (cnum != last_cnum) then
            raise CollisionError, "`#{key}(#{type})' is changed (cnum: #{cnum.inspect} -> #{last_cnum.inspect}) by other transaction and this transaction may be retried."
          end
        end
        self
      end
    end

    def initialize
      @tx_lock = ReadWriteLock.new
    end

    def exclusive
      r = nil
      @tx_lock.write_lock.synchronize{
        r = yield
      }
      r
    end
  end

  class GiantLockManager < LockManager
    def initialize(*args)
      super
      @write_lock = Mutex.new
    end

    def transaction(read_only=false)
      r = nil
      @tx_lock.read_lock.synchronize{
        if (read_only) then
          r = yield(NoWorkLockHandler.instance)
        else
          @write_lock.synchronize{
            r = yield(NoWorkLockHandler.instance)
          }
        end
      }
      r
    end
  end

  class OptimisticLockManager < LockManager
    def initialize(*args)
      super
      @critical_lock = Mutex.new
    end

    def transaction(read_only=false)
      r = nil
      @tx_lock.read_lock.synchronize{
        if (read_only) then
          lock_handler = CriticalRegionLockHandler.new(@critical_lock)
        else
          lock_handler = CollisionCheckLockHandler.new(@critical_lock)
        end
        r = yield(lock_handler)
      }
      r
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
