# $Id$

require 'thread'

module Tank
  module Thread
    # for ident(1)
    CVS_ID = '$Id$'

    class Latch
      def initialize
        @lock = Mutex.new
        @cond = ConditionVariable.new
        @start = false
      end

      def start
        @lock.synchronize{
          @start = true
          @cond.broadcast
        }
        nil
      end

      def wait
        @lock.synchronize{
          until (@start)
            @cond.wait(@lock)
          end
        }
        nil
      end
    end

    class Barrier
      def initialize(count)
        @count = count
        @lock = Mutex.new
        @cond = ConditionVariable.new
      end

      def wait
        @lock.synchronize{
          @count -= 1
          if (@count > 0) then
            while (@count > 0)
              @cond.wait(@lock)
            end
          else
            @cond.broadcast
          end
        }
        nil
      end
    end

    class SharedWork
      def initialize(&work)
        @work = work
        @lock = Mutex.new
        @cond = ConditionVariable.new
        @state = :init
        @result = nil
      end

      def result
        @lock.synchronize{
          case (@state)
          when :done
            return @result
          when :working
            until (@state == :done)
              @cond.wait(@lock)
            end
            return @result
          when :init
            @state = :working
            # fall through
          else
            raise 'internal error'
          end
        }
        @result = @work.call
        @lock.synchronize{
          @state = :done
          @cond.broadcast
        }
        @result
      end
    end
  end
end
