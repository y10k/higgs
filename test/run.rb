#!/usr/local/bin/ruby

$: << File.join(File.dirname($0), '..', 'lib')

require 'tank'

module Tank::Test
  # for ident(1)
  CVS_ID = '$Id$'
end

test_dir, this_name = File.split(__FILE__)
Dir.foreach(test_dir) do |test_rb|
  case (test_rb)
  when this_name
    # skip
  when /^test_.*\.rb$/
    require File.join(test_dir, test_rb)
  end
end
