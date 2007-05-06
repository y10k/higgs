# $Id$

require 'forwardable'
require 'higgs/storage'
require 'higgs/tman'

module Higgs
  class Store
    # for ident(1)
    CVS_ID = '$Id$'

    extend Forwardable

    DECODE = proc{|r| Marshal.load(r) }
    ENCODE = proc{|w| Marshal.dump(w) }

    def initialize(name, options={})
      @storage = Storage.new(name, options)
      options[:decode] = DECODE
      options[:encode] = ENCODE
      @tman = TransactionManager.new(@storage, options)
    end

    def_delegator :@storage, :name
    def_delegator :@storage, :number_of_read_io
    def_delegator :@storage, :jlog_sync
    def_delegator :@storage, :jlog_rotate_size
    def_delegator :@storage, :jlog_rotate_max
    def_delegator :@storage, :jlog_rotate_service_uri
    def_delegator :@storage, :shutdown
    def_delegator :@storage, :shutdown?
    def_delegator :@storage, :rotate_journal_log
    def_delegator :@storage, :verify

    def_delegator :@tman, :read_only
    def_delegator :@tman, :transaction

    def self.open(*args)
      store = new(*args)
      begin
        r = yield(store)
      ensure
        store.shutdown
      end
      r
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
