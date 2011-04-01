# -*- coding: utf-8 -*-

require 'forwardable'
require 'higgs/thread'
require 'thread'

module Higgs
  # = cache by Least Recently Used strategy
  class LRUCache
    extend Forwardable

    def initialize(limit_size=1000)
      @limit_size = limit_size
      @hash = {}
    end

    attr_reader :limit_size

    def [](key)
      if (@hash.key? key) then
        @hash[key] = @hash.delete(key)
      end
    end

    def []=(key, value)
      @hash.delete(key)
      @hash[key] = value
      if (@hash.size > @limit_size) then
        @hash.each_key do |key|
          @hash.delete(key)
          break
        end
      end
      value
    end

    def_delegator :@hash, :keys
    def_delegator :@hash, :key?
    def_delegator :@hash, :delete
    alias has_key? key?
    alias include? key?
  end

  # = multi-thread safe cache
  class SharedWorkCache
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
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
