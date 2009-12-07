# -*- coding: utf-8 -*-
# = storage like pstore
# == license
#   :include:../LICENSE
#

require 'higgs/sman'

module Higgs
  # = storage like pstore
  # == tutorial
  #
  # === 1. open a new store
  #
  #   % irb
  #   irb(main):001:0> require 'higgs'
  #   => true
  #   irb(main):002:0> st = Higgs::Store.new('foo')
  #   => #<Higgs::Store:0xb7cac7d8
  #
  # === 2. write an object
  #
  #   irb(main):003:0> st.transaction{|tx|
  #   irb(main):004:1*   tx[:foo] = Object.new
  #   irb(main):005:1> }
  #   => #<Object:0xb7d18adc>
  #
  # === 3. read an object
  #
  #   irb(main):006:0> st.transaction{|tx|
  #   irb(main):007:1*   tx[:foo]
  #   irb(main):008:1> }
  #   => #<Object:0xb7a3b1e8>
  #
  # === 4. custom property
  #
  # custom property name shuold be a string.
  #
  #   irb(main):009:0> st.transaction{|tx|
  #   irb(main):010:1*   tx.set_property(:foo, 'list', %w[ apple banana orange ])
  #   irb(main):011:1> }
  #   => nil
  #   irb(main):012:0> st.transaction{|tx|
  #   irb(main):013:1*   tx.property(:foo, 'list')
  #   irb(main):014:1> }
  #   => ["apple", "banana", "orange"]
  #
  # === 4. write a string with <tt>:string_only</tt> property
  # 
  # <tt>:string_only</tt> is system property.
  # system property name should be a symbol.
  #
  #   irb(main):015:0> st.transaction{|tx|
  #   irb(main):016:1*   tx['bar'] = "Hello world.\n"
  #   irb(main):017:1>   tx.set_property('bar', :string_only, true)
  #   irb(main):018:1> }
  #   => nil
  #
  # === 5. read a string
  #
  #   irb(main):019:0> st.transaction{|tx|
  #   irb(main):020:1*   tx['bar']
  #   irb(main):021:1> }
  #   => "Hello world.\n"
  #
  # === 6. <tt>:string_only</tt> property rejects an object
  #
  #   irb(main):022:0> st.transaction{|tx|
  #   irb(main):023:1*   tx['bar'] = Object.new
  #   irb(main):024:1> }
  #   TypeError: can't convert Object (value) to String
  #
  # === 7. read-only transaction
  #
  #   irb(main):025:0> st.transaction{|tx|
  #   irb(main):026:1*   p tx[:foo]
  #   irb(main):027:1>   p tx.property(:foo, 'list')
  #   irb(main):028:1>   p tx['bar']
  #   irb(main):029:1>   p tx.property('bar', :string_only)
  #   irb(main):030:1> }
  #   #<Object:0xb79dd764>
  #   ["apple", "banana", "orange"]
  #   "Hello world.\n"
  #   true
  #   => nil
  #
  # === 8. shutdown
  #
  #   irb(main):031:0> st.shutdown
  #   => nil
  #   irb(main):032:0> exit
  #
  # === 9. unix TAR storage and some files
  #
  #   % ls -ln
  #   total 20
  #   -rw-r----- 1 1000 1000 1024 Sep 24 20:59 foo.idx
  #   -rw-r----- 1 1000 1000 4096 Sep 24 20:59 foo.jlog
  #   -rw-r----- 1 1000 1000    0 Sep 24 20:57 foo.lock
  #   -rw-r--r-- 1 1000 1000   57 Sep 24 20:57 foo.log
  #   -rw-r----- 1 1000 1000 5120 Sep 24 20:58 foo.tar
  #
  # === 10. unix TAR contents
  #
  #   % tar tvf foo.tar
  #   -rw-r--r-- 1000/1000        12 2007-09-24 20:58 foo
  #   -rw-r--r-- 1000/1000       316 2007-09-24 20:58 foo.p
  #   -rw-r--r-- 1000/1000        13 2007-09-24 20:58 bar
  #   -rw-r--r-- 1000/1000       250 2007-09-24 20:58 bar.p
  #   % tar xvf foo.tar
  #   foo
  #   foo.p
  #   bar
  #   bar.p
  #
  # === 11. marshaled object
  #
  # `foo' is marshaled object
  #
  #   % ls -ln foo
  #   -rw-r--r-- 1 1000 1000 12 Sep 24 20:58 foo
  #   % od -c foo
  #   0000000 004  \b   o   :  \v   O   b   j   e   c   t  \0
  #   0000014
  #
  # `foo.p' is properties.
  # <tt>"list"</tt> of custom property is defined.
  #
  #   % ls -ln foo.p
  #   -rw-r--r-- 1 1000 1000 316 Sep 24 20:58 foo.p
  #   % cat foo.p
  #   # SUM16 21993
  #   ---
  #   custom_properties:
  #     list:
  #     - apple
  #     - banana
  #     - orange
  #   system_properties:
  #     hash_type: MD5
  #     modified_time: &id001 2007-09-24 20:58:03.568701 +09:00
  #     hash_value: 5490d172635c560daaff1de92779d605
  #     created_time: *id001
  #     changed_time: 2007-09-24 20:58:24.502465 +09:00
  #     string_only: false
  #
  # === 12. string with <tt>:string_only</tt> property
  #
  # `bar' is a string
  #
  #   % ls -ln bar
  #   -rw-r--r-- 1 1000 1000 13 Sep 24 20:58 bar
  #   % od -c bar
  #   0000000   H   e   l   l   o       w   o   r   l   d   .  \n
  #   0000015
  #   % cat bar
  #   Hello world.
  #
  # `bar.p' is properties.
  # <tt>:string_only</tt> of system property is <tt>true</tt>.
  #
  #   % ls -ln bar.p
  #   -rw-r--r-- 1 1000 1000 250 Sep 24 20:58 bar.p
  #   % cat bar.p
  #   # SUM16 18034
  #   ---
  #   custom_properties: {}
  #   
  #   system_properties:
  #     hash_type: MD5
  #     modified_time: &id001 2007-09-24 20:58:45.420319 +09:00
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
  # result of sample script.
  #   % ruby count.rb
  #   start - Tue Oct 02 00:50:14 +0900 2007
  #   nil
  #   
  #   1
  #   70
  #   134
  #   200
  #   256
  #   318
  #   382
  #   446
  #   508
  #   554
  #   620
  #   685
  #   751
  #   796
  #   862
  #   927
  #   992
  #   
  #   last - Tue Oct 02 00:50:15 +0900 2007
  #   1000
  #   %
  #
  class Store < StorageManager
    DECODE = proc{|r| Marshal.load(r) }
    ENCODE = proc{|w| Marshal.dump(w) }

    # see Higgs::StorageManager.new for <tt>name</tt> and <tt>options</tt>.
    def initialize(name, options={})
      options = options.dup
      options[:decode] = DECODE
      options[:encode] = ENCODE
      super(name, options)
    end
  end
end

# Local Variables:
# mode: Ruby
# indent-tabs-mode: nil
# End:
