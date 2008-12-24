# = cache utilities
#
# Author:: $Author$
# Date:: $Date::                           $
# Revision:: $Revision$
#
# == license
#   :include:../LICENSE
#

require 'forwardable'
require 'higgs/thread'
require 'thread'

module Higgs
  # = cache by Least Recently Used strategy
  class LRUCache
    # for ident(1)
    CVS_ID = '$Id$'

    extend Forwardable

    def initialize(limit_size=1000)
      @limit_size = limit_size
      @count = 0
      @cache = {}
    end

    attr_reader :limit_size
    def_delegator :@cache, :keys
    def_delegator :@cache, :key?
    alias has_key? key?
    alias include? key?

    def [](key)
      if (cached_pair = @cache[key]) then
        cached_pair[1] = @count
        @count = @count.succ
        return cached_pair[0]
      end
      nil
    end

    def []=(key, value)
      if (cached_pair = @cache[key]) then
        cached_pair[1] = @count
      else
        @cache[key] = [ value, @count ]
      end
      @count = @count.succ
      if (@cache.size > @limit_size) then
        purge_old_cache
      end
      value
    end

    def purge_old_cache
      c_list = @cache.map{|key, (value, c)| c }
      c_list.sort!
      threshold = c_list[c_list.size / 2]
      @cache.delete_if{|key, (value, c)| c < threshold }
    end
    private :purge_old_cache

    def delete(key)
      if (cached_pair = @cache.delete(key)) then
        return cached_pair[0]
      end
      nil
    end
  end

  # = multi-thread safe cache
  class SharedWorkCache
    # for ident(1)
    CVS_ID = '$Id$'

    def initialize(cache={}, &work)
      unless (work) then
        raise ArgumentError, 'required work block'
      end
      @work = work
      @lock = Mutex.new
      @cache = cache
    end

    def fetch_work(key)
      @lock.synchronize{
        if (@cache.key? key) then
          @cache[key]
        else
          @cache[key] = SharedWork.new{ @work.call(key) } 
        end
      }
    end
    private :fetch_work

    def [](key)
      fetch_work(key).result
    end

    def []=(key, value)
      work = fetch_work(key)
      work.result = value
    end

    def delete(key)
      r = @lock.synchronize{ @cache.delete(key) }
      r ? true : false
    end
  end

  # = cache for Multi-Version Concurrency Control
  class MVCCCache
    extend Forwardable

    def initialize
      @lock = Mutex.new
      @snapshots = {}
    end

    # for debug
    def_delegator :@snapshots, :empty?

    def write_old_values(cnum, write_list)
      # new snapshot is not created until the update ends.
      snapshot_list = @lock.synchronize{ @snapshots.values }

      for snapshot in snapshot_list
        @snapshots.each_value do |snapshot|
          lock = snapshot[:lock].write_lock
          cache = snapshot[:cache]
          lock.synchronize{
            for ope, key, type, value in write_list
              case (ope)
              when :value
                if (cache.key? key) then
                  if (cache[key] != :none && ! (cache[key].key? type)) then
                    cache[key][type] = { :value => value, :cnum => cnum }
                  end
                else
                  cache[key] = { type => { :value => value, :cnum => cnum } }
                end
              when :none
                unless (cache.key? key) then
                  cache[key] = :none
                end
              else
                raise "unknown operation: #{ope}"
              end
            end
          }
        end
      end

      nil
    end

    def ref_count_up(cnum_func)
      @lock.synchronize{
        cnum = cnum_func.call
        @snapshots[cnum] = { :lock => ReadWriteLock.new, :cache => {}, :ref_count => 0 } unless (@snapshots.key? cnum)
        @snapshots[cnum][:ref_count] += 1
        return cnum, @snapshots[cnum][:lock].read_lock, @snapshots[cnum][:cache]
      }
    end

    def ref_count_down(cnum)
      @lock.synchronize{
        @snapshots[cnum][:ref_count] -= 1
        @snapshots.delete(cnum) if (@snapshots[cnum][:ref_count] == 0)
      }
      nil
    end

    class Snapshot
      def initialize(parent)
        @parent = parent
      end

      def ref_count_up(cnum_func)
        @cnum, @lock, @cache = @parent.ref_count_up(cnum_func)
        nil
      end

      def ref_count_down
        @cnum or raise 'not initialized'
        @parent.ref_count_down(@cnum)
        @cnum = @lock = @cache = nil
      end

      def change_number
        @cnum
      end

      def cached?(key)
        @cache.key? key
      end

      def cache_key?(key)
        (@cache.key? key) && (@cache[key] != :none)
      end

      def each_cache_key
        @cache.each_key do |key|
          if (@cache[key] != :none) then
            yield(key)
          end
        end
        nil
      end

      def key?(store, key)
        (store.key? key) && ! (cached? key) || (cache_key? key)
      end

      def keys(store)
        key_set = {}
        store.each_key do |key|
          unless (cached? key) then
            key_set[key] = true
          end
        end
        each_cache_key do |key|
          key_set[key] = true
        end
        key_set.keys
      end

      def each_key(store)
        for key in keys(store)
          yield(key)
        end
        nil
      end

      def fetch(key, type)
        @lock.synchronize{
          if (@cache.key? key) then
            if (@cache[key] != :none) then
              if (cache_entry = @cache[key][type]) then
                cache_entry[:value]
              else
                yield
              end
            else
              nil
            end
          else
            yield
          end
        }
      end

      def write_old_values(write_list)
        @parent.write_old_values(@cnum, write_list)
        nil
      end
    end

    def transaction(cnum_func)
      r = nil
      snapshot = Snapshot.new(self)
      snapshot.ref_count_up(cnum_func)
      begin
        r = yield(snapshot)
      ensure
        snapshot.ref_count_down
      end
      r
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
