# cache utilities

require 'forwardable'
require 'higgs/thread'
require 'thread'

module Higgs
  # cache by Least Recently Used strategy
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

  # multi-thread safe cache
  class SharedWorkCache
    # for ident(1)
    CVS_ID = '$Id$'

    def initialize(cache={}, &work)
      unless (work) then
        raise 'required work block'
      end
      @work = work
      @lock = Mutex.new
      @cache = cache
    end

    def [](key)
      work = nil
      @lock.synchronize{
        unless (@cache.key? key) then
          @cache[key] = SharedWork.new{ @work.call(key) } 
        end
        work = @cache[key]
      }
      work.result
    end

    def delete(key)
      r = @lock.synchronize{ @cache.delete(key) }
      r ? true : false
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
