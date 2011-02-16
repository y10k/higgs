# -*- coding: utf-8 -*-

require 'forwardable'
require 'higgs/block'
require 'higgs/thread'
require 'thread'

module Higgs
  # = storage index
  class Index
    extend Forwardable
    include Synchronized

    MAGIC_SYMBOL = 'HIGGS_INDEX'
    MAJOR_VERSION = 0
    MINOR_VERSION = 3

    def initialize
      @change_number = 0
      @eoa = 0
      @free_lists = {}
      @index = {}
      @storage_id = nil
      self.__lock__ = Mutex.new
    end

    synchronized_reader :change_number
    synchronized_accessor :eoa
    synchronized_accessor :storage_id

    def succ!
      @change_number = @change_number.succ
      self
    end
    synchronized :succ!

    def free_fetch(size)
      @free_lists[size].shift if (@free_lists.key? size)
    end
    synchronized :free_fetch

    def free_fetch_at(pos, size)
      @free_lists[size].delete(pos) or raise "not found free region: (#{pos},#{size})"
    end
    synchronized :free_fetch_at

    def free_store(pos, size)
      @free_lists[size] = [] unless (@free_lists.key? size)
      @free_lists[size] << pos
      nil
    end
    synchronized :free_store

    def_synchronized_delegator :@index, :[]
    def_synchronized_delegator :@index, :[]=
    def_synchronized_delegator :@index, :delete
    def_synchronized_delegator :@index, :key?
    def_synchronized_delegator :@index, :keys

    def each_key
      keys = __lock__.synchronize{ @index.keys }
      for key in keys
        yield(key)
      end
      self
    end

    def to_h
      { :version => [ MAJOR_VERSION, MINOR_VERSION ],
        :change_number => @change_number,
        :eoa => @eoa,
        :free_lists => @free_lists,
        :index => @index,
        :storage_id => @storage_id
      }
    end
    synchronized :to_h

    class << self
      # backward compatibility for migration.
      def create_id(key, identities)
        id = key.to_s
        if (identities.key? id) then
          id += '.a'
          id.succ! while (identities.key? id)
        end
        id
      end
      private :create_id

      def migration_0_0_to_0_1(index_data)
        if ((index_data[:version] <=> [ 0, 0 ]) > 0) then
          return
        end
        if ((index_data[:version] <=> [ 0, 0 ]) < 0) then
          raise "unexpected index format version: #{index_data[:version].join('.')}"
        end

        index = index_data[:index]
        identities = index_data[:identities] = {}
        for key in index.keys
          id = create_id(key, identities)
          identities[id] = key
          value = index[key]
          index[key] = [ id, value ]
        end
        index_data[:version] = [ 0, 1 ]

        index_data
      end
      private :migration_0_0_to_0_1

      def migration_0_1_to_0_2(index_data, idx)
        if ((index_data[:version] <=> [ 0, 1 ]) > 0) then
          return
        end
        if ((index_data[:version] <=> [ 0, 1 ]) < 0) then
          raise "unexpected index format version: #{index_data[:version].join('.')}"
        end

        index_data[:storage_id] = idx.storage_id
        index_data[:version] = [ 0, 2 ]

        index_data
      end
      private :migration_0_1_to_0_2

      def migration_0_2_to_0_3(index_data)
        if ((index_data[:version] <=> [ 0, 2 ]) > 0) then
          return
        end
        if ((index_data[:version] <=> [ 0, 2 ]) < 0) then
          raise "unexpected index format version: #{index_data[:version].join('.')}"
        end

        index = index_data[:index]
        for key in index.keys
          index[key] = index[key][1]
        end
        index_data.delete(:identities)
        index_data[:version] = [ 0, 3 ]

        index_data
      end
      private :migration_0_2_to_0_3

      def migration(index, index_data)
        migration_0_0_to_0_1(index_data)
        migration_0_1_to_0_2(index_data, index)
        migration_0_2_to_0_3(index_data)
      end
    end

    def save(path)
      tmp_path = "#{path}.tmp.#{$$}"
      File.open(tmp_path, File::WRONLY | File::CREAT | File::TRUNC, 0660) {|f|
        f.binmode
        f.set_encoding(Encoding::ASCII_8BIT)
        Block.block_write(f, MAGIC_SYMBOL,
                          __lock__.synchronize{
                            Marshal.dump(thread_unsafe_to_h)
                          })
        f.fsync
      }
      File.rename(tmp_path, path)
      self
    end

    class << self
      def load_data(path)
        File.open(path, 'r') {|f|
          f.binmode
          f.set_encoding(Encoding::ASCII_8BIT)
          Marshal.load(Block.block_read(f, MAGIC_SYMBOL))
        }
      end
    end

    def replace_data(index_data)
      if (index_data[:version] != [ MAJOR_VERSION, MINOR_VERSION ]) then
        raise "unsupported version: #{index_data[:version].join('.')}"
      end
      __lock__.synchronize{
        @change_number = index_data[:change_number]
        @eoa = index_data[:eoa]
        @free_lists = index_data[:free_lists]
        @index = index_data[:index]
        @storage_id = index_data[:storage_id]
      }
      self
    end

    def load(path)
      index_data = self.class.load_data(path)
      self.class.migration(self, index_data)
      replace_data(index_data)
      self
    end
  end

  # = new MVCC storage index
  class MVCCIndex
    # :stopdoc:
    module EditUtils
      def get_entry(cnum, entry_alist)
        if (entry_pair = entry_alist.find{|c, e| c <= cnum}) then
          entry_pair[1]
        end
      end
      module_function :get_entry

      def put_entry(cnum, entry_alist, new_entry)
        new_cnum = cnum.succ
        if (entry_alist.empty?) then
          # at first putting, nil value is placed to current change
          # number point corresponding to its update mark. nil value
          # means empty because index entry will not be nil.
          entry_alist = [ [ new_cnum, new_entry ], [ cnum, nil ] ]
        else
          entry_alist = entry_alist.dup
          entry_alist.unshift([ new_cnum, new_entry ])
        end

        entry_alist
      end
      module_function :put_entry

      def make_update_entry(cnum)
        { :cnum => cnum,
          :update_marks => {},
          :free_list_logs => [],
          :ref_count => 0
        }
      end
      module_function :make_update_entry
    end
    include EditUtils
    # :startdoc:

    include Synchronized

    MAGIC_SYMBOL = 'HIGGS_INDEX'
    MAJOR_VERSION = 0
    MINOR_VERSION = 4

    def initialize
      @change_number = 0
      @eoa = 0
      @free_lists = {}
      @index = {}
      @update_queue = [ make_update_entry(0) ]
      @storage_id = nil
      self.__lock__ = Mutex.new
    end

    synchronized_reader :change_number
    synchronized_accessor :eoa
    synchronized_accessor :storage_id

    def succ!
      @change_number = @change_number.succ
      @update_queue.push(make_update_entry(@change_number))
      self
    end
    synchronized :succ!

    def clear_old_entries
      while (@update_queue.length > 1 && @update_queue.first[:ref_count] == 0)
        update_log = @update_queue.shift
        update_log[:update_marks].each_key do |key|
          entry_alist = @index[key]

          # assertion
          (entry_alist.length >= 2) or raise 'internal error.'
          (entry_alist.last[0] <= update_log[:cnum]) or raise 'internal error.'

          entry_alist = entry_alist.dup
          entry_alist.pop
          if (entry_alist.length == 1 && entry_alist[0][1] == nil) then
            @index.delete(key)
          else
            @index[key] = entry_alist
          end
        end
        update_log[:free_list_logs].each do |jlog|
          @free_lists[jlog[:siz]] = [] unless (@free_lists.key? jlog[:siz])
          @free_lists[jlog[:siz]] << jlog[:pos]
        end
      end

      nil
    end
    private :clear_old_entries

    def transaction
      update_log = nil
      cnum = nil

      __lock__.synchronize{
        update_log = @update_queue.last

        # assertion
        (update_log[:cnum] == @change_number) or raise 'internal error.'
        (update_log[:ref_count] >= 0) or raise 'internal error.'

        update_log[:ref_count] += 1
        cnum = update_log[:cnum]
      }

      begin
        r = yield(cnum)
      ensure
        __lock__.synchronize{
          update_log[:ref_count] -= 1
          clear_old_entries
        }
      end

      r
    end

    def free_fetch(size)
      @free_lists[size].shift if (@free_lists.key? size)
    end
    synchronized :free_fetch

    def free_fetch_at(pos, size)
      @free_lists[size].delete(pos) or raise "not found free region: (#{pos},#{size})"
    end
    synchronized :free_fetch_at

    def free_store(pos, size)
      update_log = @update_queue.last

      # assertion
      (update_log[:cnum] == @change_number) or raise 'internal error.'
      (update_log[:ref_count] >= 0) or raise 'internal error.'

      update_log[:free_list_logs] << { :pos => pos, :siz => size }

      nil
    end
    synchronized :free_store

    def [](cnum, key)
      # assertion
      # allowed next transaction access.
      (@update_queue.first[:cnum] <= cnum && cnum <= @change_number.succ) or raise 'internal error.'

      if (entry_alist = @index[key]) then
        get_entry(cnum, entry_alist)
      end
    end
    synchronized :[]

    def []=(cnum, key, value)
      update_log = @update_queue.last

      # assertion
      (update_log[:cnum] == @change_number) or raise 'internal error.'
      (update_log[:ref_count] >= 0) or raise 'internal error.'

      update_log[:update_marks][key] = true
      new_entry_alist = put_entry(cnum, @index[key] || [], value)
      @index[key] = new_entry_alist

      value
    end
    synchronized :[]=

    def delete(cnum, key)
      update_log = @update_queue.last

      # assertion
      (update_log[:cnum] == @change_number) or raise 'internal error.'
      (update_log[:ref_count] >= 0) or raise 'internal error.'

      if (entry_alist = @index[key]) then
        if (value = get_entry(cnum, entry_alist)) then
          update_log[:update_marks][key] = true
          new_entry_alist = put_entry(cnum, entry_alist, nil)
          @index[key] = new_entry_alist
          return value
        end
      end

      nil
    end
    synchronized :delete

    def key?(cnum, key)
      self[cnum, key] ? true : false
    end

    def keys(cnum)
      key_list = __lock__.synchronize{ @index.keys }
      key_list.delete_if{|key| ! key?(cnum, key) }
      key_list
    end

    def each_key(cnum)
      key_list = __lock__.synchronize{ @index.keys }
      for key in key_list
        yield(key) if key?(cnum, key)
      end
      self
    end

    IndexBlockElement = Struct.new(:block_type, :key, :entry, :change_number)
    FreeBlockElement = Struct.new(:block_type, :pos, :size)
    FreeLogBlockElement = Struct.new(:block_type, :pos, :size, :change_number)

    # all index operations are blocked while this method is
    # processing.
    def each_block_element
      for key, entry_alist in @index
        for cnum, entry in entry_alist
          if (entry) then
            yield(IndexBlockElement.new(:index, key, entry, cnum))
          end
        end
      end
      for size, free_list in @free_lists
        for pos in free_list
          yield(FreeBlockElement.new(:free, pos, size))
        end
      end
      for update_log in @update_queue
        for jlog in update_log[:free_list_logs]
          yield(FreeLogBlockElement.new(:free_log, jlog[:pos], jlog[:siz], update_log[:cnum]))
        end
      end

      self
    end
    synchronized :each_block_element

    # notation: this method is thread unsafe.
    # index internal data may be updated by other threads.
    def to_h
      { :version => [ MAJOR_VERSION, MINOR_VERSION ],
        :change_number => @change_number,
        :eoa => @eoa,
        :free_lists => @free_lists,
        :index => @index,
        :update_queue => @update_queue,
        :storage_id => @storage_id
      }
    end

    def replace_data(index_data)
      if (index_data[:version] != [ MAJOR_VERSION, MINOR_VERSION ]) then
        raise "unsupported version: #{index_data[:version].join('.')}"
      end
      __lock__.synchronize{
        @change_number = index_data[:change_number]
        @eoa = index_data[:eoa]
        @free_lists = index_data[:free_lists]
        @index = index_data[:index]
        @update_queue = index_data[:update_queue]
        @storage_id = index_data[:storage_id]
      }
      self
    end

    def save(path)
      tmp_path = "#{path}.tmp.#{$$}"
      File.open(tmp_path, File::WRONLY | File::CREAT | File::TRUNC, 0660) {|f|
        f.binmode
        f.set_encoding(Encoding::ASCII_8BIT)
        Block.block_write(f, MAGIC_SYMBOL,
                          __lock__.synchronize{ Marshal.dump(to_h) })
        f.fsync
      }
      File.rename(tmp_path, path)
      self
    end

    class << self
      # backward compatibility for migration.
      def create_id(key, identities)
        id = key.to_s
        if (identities.key? id) then
          id += '.a'
          id.succ! while (identities.key? id)
        end
        id
      end
      private :create_id

      def migration_0_0_to_0_1(index_data)
        if ((index_data[:version] <=> [ 0, 0 ]) > 0) then
          return
        end
        if ((index_data[:version] <=> [ 0, 0 ]) < 0) then
          raise "unexpected index format version: #{index_data[:version].join('.')}"
        end

        index = index_data[:index]
        identities = index_data[:identities] = {}
        for key in index.keys
          id = create_id(key, identities)
          identities[id] = key
          value = index[key]
          index[key] = [ id, value ]
        end
        index_data[:version] = [ 0, 1 ]

        index_data
      end
      private :migration_0_0_to_0_1

      def migration_0_1_to_0_2(index_data, idx)
        if ((index_data[:version] <=> [ 0, 1 ]) > 0) then
          return
        end
        if ((index_data[:version] <=> [ 0, 1 ]) < 0) then
          raise "unexpected index format version: #{index_data[:version].join('.')}"
        end

        index_data[:storage_id] = idx.storage_id
        index_data[:version] = [ 0, 2 ]

        index_data
      end
      private :migration_0_1_to_0_2

      def migration_0_2_to_0_3(index_data)
        if ((index_data[:version] <=> [ 0, 2 ]) > 0) then
          return
        end
        if ((index_data[:version] <=> [ 0, 2 ]) < 0) then
          raise "unexpected index format version: #{index_data[:version].join('.')}"
        end

        index = index_data[:index]
        for key in index.keys
          index[key] = index[key][1]
        end
        index_data.delete(:identities)
        index_data[:version] = [ 0, 3 ]

        index_data
      end
      private :migration_0_2_to_0_3

      def migration_0_3_to_0_4(index_data)
        if ((index_data[:version] <=> [ 0, 3 ]) > 0) then
          return
        end
        if ((index_data[:version] <=> [ 0, 3 ]) < 0) then
          raise "unexpected index format version: #{index_data[:version].join('.')}"
        end

        cnum = index_data[:change_number]
        index = index_data[:index]
        for key in index.keys
          index[key] = [ [ cnum, index[key] ] ]
        end
        index_data[:update_queue] = [ EditUtils.make_update_entry(cnum) ]
        index_data[:version] = [ 0, 4 ]

        index_data
      end
      private :migration_0_3_to_0_4

      def migration(index, index_data)
        migration_0_0_to_0_1(index_data)
        migration_0_1_to_0_2(index_data, index)
        migration_0_2_to_0_3(index_data)
        migration_0_3_to_0_4(index_data)
      end
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
