# multi-thread utilities

require 'forwardable'
require 'higgs/exceptions'
require 'thread'

module Higgs
  class Latch
    # for ident(1)
    CVS_ID = '$Id$'

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
    # for ident(1)
    CVS_ID = '$Id$'

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
    # for ident(1)
    CVS_ID = '$Id$'

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
    # for ident(1)
    CVS_ID = '$Id$'

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
    # for ident(1)
    CVS_ID = '$Id$'

    def initialize
      @lock = Mutex.new
      @read_cond = ConditionVariable.new
      @write_cond = ConditionVariable.new
      @count_of_working_readers = 0
      @count_of_standby_writers = 0
      @priority_to_writer = true
      @writing = false
    end

    def __read_lock__
      @lock.synchronize{
        while (@writing || (@priority_to_writer && @count_of_standby_writers > 0))
          @read_cond.wait(@lock)
        end
        @count_of_working_readers += 1
      }
      nil
    end

    def __read_try_lock__
      @lock.synchronize{
        if (@writing || (@priority_to_writer && @count_of_standby_writers > 0)) then
          return false
        else
          @count_of_working_readers += 1
          return true
        end
      }
    end

    def __read_unlock__
      @lock.synchronize{
        @count_of_working_readers -= 1
        @priority_to_writer = true
        if (@count_of_standby_writers > 0) then
          @write_cond.signal
        else
          @read_cond.broadcast
        end
      }
      nil
    end

    def __write_lock__
      @lock.synchronize{
        @count_of_standby_writers += 1
        begin
          while (@writing || @count_of_working_readers > 0)
            @write_cond.wait(@lock)
          end
          @writing = true
        ensure
          @count_of_standby_writers -= 1
        end
      }
      nil
    end

    def __write_try_lock__
      @lock.synchronize{
        @count_of_standby_writers += 1
        begin
          if (@writing || @count_of_working_readers > 0) then
            return false
          else
            @writing = true
            return true
          end
        ensure
          @count_of_standby_writers -= 1
        end
      }
      nil
    end

    def __write_unlock__
      @lock.synchronize{
        @writing = false
        @priority_to_writer = false
        @read_cond.broadcast
        if (@count_of_standby_writers > 0) then
          @write_cond.signal
        end
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

    def to_a
      [ read_lock, write_lock ]
    end
  end

  class Pool
    # for ident(1)
    CVS_ID = '$Id$'

    class ShutdownException < Exceptions::ShutdownException
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
            @q_cond.signal    # for shutdown
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

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
