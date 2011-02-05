# -*- coding: utf-8 -*-

require 'forwardable'
require 'higgs/exceptions'
require 'thread'

module Higgs
  module Synchronized
    def self.included(mod)
      r = super
      mod.extend(SynchronizedSyntax)
      r
    end

    attr_accessor :__lock__
  end

  module SynchronizedSyntax
    def synchronized(name, *optional_names)
      names = [ name ] + optional_names
      for name in names
        name = name.to_sym
        thread_unsafe_name = "thread_unsafe_#{name}".to_sym

        if (public_instance_methods(true).any?{|n| n.to_sym ==  name }) then
          visibility = :public
        elsif (private_instance_methods(true).any?{|n| n.to_sym == name }) then
          visibility = :private
        elsif (protected_instance_methods(true).any?{|n| n.to_sym == name }) then
          visibility = :protected
        else
          raise NoMethodError, "undefined method `#{name}' for #{self}"
        end

        # block for local scope.
        class_eval{
          alias_method thread_unsafe_name, name
          private thread_unsafe_name

          orig_method = thread_unsafe_name
          define_method name, lambda{|*args, &block|
            @__lock__.synchronize{
              __send__(orig_method, *args, &block)
            }
          }

          case (visibility)
          when :public
            public name
          when :private
            private name
          when :protected
            protected name
          else
            raise 'internal error.'
          end
        }
      end

      nil
    end
    private :synchronized

    def synchronized_attr(name, assignable=false)
      r = attr(name, assignable)
      synchronized(name)
      synchronized("#{name}=") if assignable
      r
    end
    private :synchronized_attr

    def synchronized_accessor(name, *optional_names)
      r = attr_accessor(name, *optional_names)
      synchronized(name, "#{name}=",
                   *(optional_names +
                     optional_names.map{|n| "#{n}=" }))
      r
    end
    private :synchronized_accessor

    def synchronized_reader(name, *optional_names)
      r = attr_reader(name, *optional_names)
      synchronized(name, *optional_names)
      r
    end
    private :synchronized_reader

    def synchronized_writer(name, *optional_names)
      r = attr_writer(name)
      synchronized("#{name}=", *optional_names.map{|n| "#{n}=" })
      r
    end
    private :synchronized_writer

    def def_synchronized_delegators(accessor, *methods)
      r = def_delegators(accessor, *methods)
      synchronized(*methods)
      r
    end
    private :def_synchronized_delegators

    def def_synchronized_delegator(accessor, method, ali=method)
      r = def_delegator(accessor, method, ali)
      synchronized(ali)
      r
    end
    private :def_synchronized_delegator
  end

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
          @cond.broadcast if (@count == 0)
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
        raise ArgumentError, 'required work block'
      end
      @work = work
      @lock = Mutex.new
      @cond = ConditionVariable.new
      @state = :init
      @abort = nil
      @error = nil
      @result = nil
    end

    def abort_msg
      msg = 'abort'
      msg << " - #{@error.inspect}" if @error
      msg
    end
    private :abort_msg

    def __result__
      if (@abort) then
        raise RuntimeError, abort_msg
      end
      @result
    end
    private :__result__

    def result
      @lock.synchronize{
        case (@state)
        when :done
          return __result__
        when :init
          @state = :working
          # fall through
        when :working
          until (@state == :done)
            @cond.wait(@lock)
          end
          return __result__
        else
          raise 'internal error'
        end
      }

      completed = false
      begin
        r = @result = @work.call
        completed = true
      ensure
        @lock.synchronize{
          @state = :done
          unless (completed) then
            @abort = true
            @error = $!
          end
          @cond.broadcast
        }
      end
      r
    end

    def result=(value)
      @lock.synchronize{
        case (@state)
        when :init
          @state = :done
        when :working
          until (@state == :done)
            @cond.wait(@lock)
          end
        when :done
          # nothing to do.
        else
          raise 'internal error'
        end
        if (@abort) then
          raise RuntimeError, abort_msg
        end
        @result = value
      }
    end
  end

  class ReadWriteLock
    def initialize
      @lock = Mutex.new
      @read_cond = ConditionVariable.new
      @write_cond = ConditionVariable.new
      @count_of_working_readers = 0
      @count_of_standby_writers = 0
      @priority_to_writer = true
      @writing = false
      @read_lock = ReadLock.new(self)
      @write_lock = WriteLock.new(self)
    end

    attr_reader :read_lock
    attr_reader :write_lock

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
      raise 'not to reach'
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

    def to_a
      [ read_lock, write_lock ]
    end
  end

  class Pool
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
