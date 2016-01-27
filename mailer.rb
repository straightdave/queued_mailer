require 'redis'
require 'json'
require 'pony'

module RedisConf
  Host = '127.0.0.1'
  Port = 6379
  DB   = 8
  List = "validation"
end

module PonyConf
  From = 'eyaswoo@163.com'
  SMTP_Options = {
    :address        => 'smtp.163.com',
    :port           => '25',
    :user_name      => 'eyaswoo',
    :password       => 'Password01!',
    :authentication => :plain, # :plain, :login, :cram_md5, no auth by default
    :domain         => "localhost.localdomain" # the HELO domain provided by the client to the server
  }
end

begin
  puts "[mailer] #{Time.now} : starting Redis client ..."
  rc = Redis.new(
    :host => RedisConf::Host,
    :port => RedisConf::Port,
    :db   => RedisConf::DB
  )
  raise "[mailer] #{Time.now} : cannot create Redis client" unless rc
rescue
  puts $!.message
  puts "[mailer] #{Time.now} : quitting ..."
end



while true
  trap("INT") {
    puts "get ctrl-c, break the loop..."
    break
  }

  begin
    puts "[mailer] #{Time.now} : waiting for next job ..."

    job = rc.brpop("validation")
    puts "[mailer] #{Time.now} : job get =>"

    mail = JSON.parse(job[1])
    puts "  list name: #{job[0]}, mail: #{mail}"
    puts "  gonna send mail to #{mail["to"]} ..."

    Pony.mail(
      :to          => mail["to"],
      :subject     => mail["subject"],
      :body        => mail["content"],
      :from        => PonyConf::From,
      :via         => :smtp,
      :via_options => PonyConf::SMTP_Options
    )
    puts "  mail sent successfully!"
  rescue
    puts "[mailer] Exception in waiting loop:"
    puts $!.message
  end
end

puts "Gracefully exit"
