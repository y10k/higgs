# $Id$

require 'tank'
require 'tank/thread'
require 'thread'

module Tank
  class SharedWorkCache
    # for ident(1)
    CVS_ID = '$Id$'

    def initialize(&work)
      @work = work
      @lock = Mutex.new
      @cache = {}
    end

    def [](key)
      work = nil
      @lock.synchronize{
        unless (@cache.include? key) then
          @cache[key] = SharedWork.new{ @work.call(key) } 
        end
        work = @cache[key]
      }
      work.result
    end

    def expire(key)
      r = @lock.synchronize{ @cache.delete(key) }
      r ? true : false
    end
  end
end
