# $Id$

require 'thread'

module Higgs
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

    class CountDownLatch
      def initialize(count)
        @count = count
        @lock = Mutex.new
        @cond = ConditionVariable.new
      end

      def count_down
        @lock.synchronize{
          if (@count > 0) then
            @count -= 1
            @cond.broadcast 
          end
        }
        nil
      end

      def wait
        @lock.synchronize{
          while (@count > 0)
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
          if (@count > 0) then
            @count -= 1
            if (@count > 0) then
              while (@count > 0)
                @cond.wait(@lock)
              end
            else
              @cond.broadcast
            end
          else
            raise 'not recycle'
          end
        }
        nil
      end
    end

    class SharedWork
      def initialize(&work)
        unless (work) then
          raise 'required work block'
        end
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

    class ReadWriteLock
      def initialize
        @lock = Mutex.new
        @cond = ConditionVariable.new
        @reading_count = 0
        @writing_just_now = false
      end

      def __read_lock__
        @lock.synchronize{
          while (@writing_just_now)
            @cond.wait(@lock)
          end
          @reading_count += 1
        }
        nil
      end

      def __read_try_lock__
        @lock.synchronize{
          if (@writing_just_now) then
            return false
          else
            @reading_count += 1
            return true
          end
        }
      end

      def __read_unlock__
        @lock.synchronize{
          @reading_count -= 1
          if (@reading_count == 0) then
            @cond.broadcast
          end
        }
        nil
      end

      def __write_lock__
        @lock.synchronize{
          while (@writing_just_now || @reading_count > 0)
            @cond.wait(@lock)
          end
          @writing_just_now = true
        }
        nil
      end

      def __write_try_lock__
        @lock.synchronize{
          if (@writing_just_now || @reading_count > 0) then
            return false
          else
            @writing_just_now = true
            return true
          end
        }
      end

      def __write_unlock__
        @lock.synchronize{
          @writing_just_now = false
          @cond.broadcast
        }
        nil
      end

      class ChildLock
        def initialize(rw_lock)
          @rw_lock = rw_lock
        end

        def synchronize
          lock
          begin
            r = yield
          ensure
            unlock
          end
          r
        end
      end

      class ReadLock < ChildLock
        extend Forwardable

        def_delegator :@rw_lock, :__read_lock__,     :lock
        def_delegator :@rw_lock, :__read_try_lock__, :try_lock
        def_delegator :@rw_lock, :__read_unlock__,   :unlock
      end

      class WriteLock < ChildLock
        extend Forwardable

        def_delegator :@rw_lock, :__write_lock__,     :lock
        def_delegator :@rw_lock, :__write_try_lock__, :try_lock
        def_delegator :@rw_lock, :__write_unlock__,   :unlock
      end

      def read_lock
        ReadLock.new(self)
      end

      def write_lock
        WriteLock.new(self)
      end
    end

    class Pool
      class ShutdownException < Exception
      end

      def initialize(size)
        @size = size
        @running = true
        @queue = []
        @q_lock = Mutex.new
        @q_cond = ConditionVariable.new
        @size.times do
          @queue << yield
        end
      end

      attr_reader :size

      def fetch
        @q_lock.synchronize{
          loop do
            unless (@running) then
              @q_cond.signal unless @queue.empty? # for shutdown
              raise ShutdownException, 'pool shutdown'
            end
            if (@queue.empty?) then
              @q_cond.wait(@q_lock)
            else
              break
            end
          end
          @queue.shift
        }
      end

      def restore(obj)
        @q_lock.synchronize{
          @queue.push(obj)
          @q_cond.signal
        }
        nil
      end

      def transaction
        obj = fetch
        begin
          r = yield(obj)
        ensure
          restore(obj)
        end
        r
      end

      def shutdown
        @size.times do
          obj = @q_lock.synchronize{
            @running = false
            while (@queue.empty?)
              @q_cond.wait(@q_lock)
            end
            @queue.shift
          }
          yield(obj) if block_given?
        end
        nil
      end
    end
  end
end
