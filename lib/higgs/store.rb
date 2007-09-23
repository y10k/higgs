# = storage like pstore
#
# Author:: $Author$
# Date:: $Date$
# Revision:: $Revision$
#
# == license
#   :include:LICENSE
#

require 'higgs/storage'
require 'higgs/tman'

module Higgs
  # = storage like pstore
  # == tutorial
  #
  # === 1. open a new store
  #
  #   irb(main):001:0> require 'higgs'
  #   => true
  #   irb(main):002:0> st = Higgs::Store.new('foo')
  #   => #<Higgs::Store:0xb7d04294
  #
  # === 2. write an object
  # 
  #   irb(main):008:0> st.transaction{|tx|
  #   irb(main):009:1*   tx[:foo] = Object.new
  #   irb(main):010:1> }
  #   => #<Object:0xb7db86b8>
  #
  # === 3. read an object
  #
  #   irb(main):011:0> st.transaction{|tx|
  #   irb(main):012:1*   tx[:foo]
  #   irb(main):013:1> }
  #   => #<Object:0xb7a9aa58>
  #
  # === 4. write a string with <tt>:string_only</tt> property
  # 
  #   irb(main):014:0> st.transaction{|tx|
  #   irb(main):015:1*   tx[:bar] = "Hello world.\n"
  #   irb(main):016:1>   tx.set_property(:bar, :string_only, true)
  #   irb(main):017:1> }
  #   => nil
  #
  # === 5. read a string
  #
  #   irb(main):018:0> st.transaction{|tx|
  #   irb(main):019:1*   tx[:bar]
  #   irb(main):020:1> }
  #   => "Hello world.\n"
  #
  # === 6. <tt>:string_only</tt> property rejects an object
  #
  #   irb(main):021:0> st.transaction{|tx|
  #   irb(main):022:1*   tx[:bar] = Object.new
  #   irb(main):023:1> }
  #   TypeError: can't convert Object (value) to String
  #
  # === 7. read-only transaction
  #
  #   irb(main):024:0> st.transaction(true) {|tx|
  #   irb(main):025:1*   p tx[:foo]
  #   irb(main):026:1>   p tx[:bar]
  #   irb(main):027:1> }
  #   #<Object:0xb7a5ec88>
  #   "Hello world.\n"
  #   => nil
  #
  # === 8. shutdown
  #
  #   irb(main):028:0> st.shutdown
  #   => nil
  #
  # === 9. unix TAR storage and some files
  #
  #   % ls -l
  #   total 20
  #   -rw-r----- 1 toki toki 1024 Sep 21 00:18 foo.idx
  #   -rw-r----- 1 toki toki 3072 Sep 21 00:18 foo.jlog
  #   -rw-r----- 1 toki toki    0 Sep 21 00:17 foo.lock
  #   -rw-r--r-- 1 toki toki   57 Sep 21 00:17 foo.log
  #   -rw-r----- 1 toki toki 5120 Sep 21 00:18 foo.tar
  #
  # === 10. unix TAR contents
  #
  #   % tar tvf foo.tar
  #   -rw-r--r-- 1000/1000        12 2007-09-21 00:17 foo
  #   -rw-r--r-- 1000/1000       251 2007-09-21 00:17 foo.p
  #   -rw-r--r-- 1000/1000        13 2007-09-21 00:18 bar
  #   -rw-r--r-- 1000/1000       250 2007-09-21 00:18 bar.p
  #   % tar xvf foo.tar
  #   foo
  #   foo.p
  #   bar
  #   bar.p
  #
  # === 11. marshaled object
  #
  # `foo' is marshaled object
  #   % ls -ln foo
  #   -rw-r--r-- 1 1000 1000 12 Sep 21 00:17 foo
  #   % od -c foo
  #   0000000 004  \b   o   :  \v   O   b   j   e   c   t  \0
  #   0000014
  #
  # `foo.p' is properties
  #   % ls -ln foo.p
  #   -rw-r--r-- 1 1000 1000 251 Sep 21 00:17 foo.p
  #   % cat foo.p
  #   # SUM16 18163
  #   --- 
  #   custom_properties: {}
  #   
  #   system_properties: 
  #     hash_type: MD5
  #     modified_time: &id001 2007-09-21 00:17:53.617973 +09:00
  #     created_time: *id001
  #     hash_value: 5490d172635c560daaff1de92779d605
  #     changed_time: *id001
  #     string_only: false
  #
  # === 12. string with <tt>:string_only</tt> property
  #
  # `bar' is a string
  #   % ls -ln bar  
  #   -rw-r--r-- 1 1000 1000 13 Sep 21 00:18 bar
  #   % cat bar
  #   Hello world.
  #
  # `bar.p' is properties (<tt>:string_only</tt> is true)
  #   % ls -ln bar.p
  #   -rw-r--r-- 1 1000 1000 250 Sep 21 00:18 bar.p
  #   % cat bar.p
  #   # SUM16 18042
  #   --- 
  #   custom_properties: {}
  #   
  #   system_properties: 
  #     hash_type: MD5
  #     modified_time: &id001 2007-09-21 00:18:19.658367 +09:00
  #     created_time: *id001
  #     hash_value: fa093de5fc603823f08524f9801f0546
  #     changed_time: *id001
  #     string_only: true
  #
  # === 13. other operations
  #
  # <tt>tx</tt> of block argument is an instance of
  # Higgs::TransactionContext. see Higgs::TransactionContext for all
  # operations of transaction.
  #
  # == sample script
  #
  # sample/count.rb
  #   :include: sample/count.rb
  #
  class Store
    # for ident(1)
    CVS_ID = '$Id$'

    include Storage::Export
    include TransactionManager::Export

    DECODE = proc{|r| Marshal.load(r) }
    ENCODE = proc{|w| Marshal.dump(w) }

    # <tt>name</tt> is a storage name and see Higgs::Storage.new for
    # detail. see Higgs::Storage::InitOptions and
    # Higgs::TransactionManager::InitOptions for <tt>options</tt>.
    def initialize(name, options={})
      options[:decode] = DECODE
      options[:encode] = ENCODE
      @storage = Storage.new(name, options)
      @tman = TransactionManager.new(@storage, options)
    end

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
