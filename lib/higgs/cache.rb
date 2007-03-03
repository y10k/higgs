# $Id$

require 'forwardable'
require 'higgs/thread'
require 'thread'

module Higgs
  module Cache
    # for ident(1)
    CVS_ID = '$Id$'

    class LRUCache
      extend Forwardable

      def initialize(limit_size=1000)
	@limit_size = limit_size
	@count = 0
	@cache = {}
      end

      def_delegator :@cache, :key?
      alias has_key? key?
      alias include? key?

      def [](key)
	if (cached_pair = @cache[key]) then
	  c = @count
	  @count += 1
	  cached_pair[1] = c
	  return cached_pair[0]
	end
	nil
      end

      def []=(key, value)
	c = @count
	@count += 1
	if (cached_pair = @cache[key]) then
	  cached_pair[1] = c
	else
	  @cache[key] = [ value, c ]
	end
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

    class SharedWorkCache
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
          unless (@cache.include? key) then
            @cache[key] = Thread::SharedWork.new{ @work.call(key) } 
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
end
