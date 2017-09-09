require 'dnssd'
require 'celluloid/current'
require 'celluloid/supervision'
require 'influxdb'

class Device
  include Celluloid

  def initialize(name)
    puts "Registered #{name}"
    @name = name
  end

  def run(influx)
    # data looks like {"error"=>false, "temp"=>21.5, "humidity"=>60.5}
    data = JSON.parse(Net::HTTP.get("#{@name}.local", '/dht'))
    # get the data
    unless data['error']
      influx.log(@name, data['temp'], data['humidity']);
    end
  end
end

class Store
  def initialize(db, table, config = {})
    @table = table
    @influx = InfluxDB::Client.new db, config
  end

  def log(name, temp, humidity)
    @influx.write_point(@table, {
      values: {
        humidity: humidity,
        temp: temp
      },
      tags: {
        sensor: name
      }
    })
  end
end

class Server
  MDNS_NAME = '_sensor._tcp.'.freeze
  LOG_FREQUENCY_SECONDS = 30
  TABLE_NAME = "sensors"
  DB_NAME = "phil"

  include Celluloid

  def initialize
    puts "server started"
    @discovered = []
    DNSSD.browse MDNS_NAME do |reply|
      @discovered.push reply.name
    end
    @timer = every(LOG_FREQUENCY_SECONDS) { spawner }
  end

  def spawner
    @discovered.each do |device_name|
      Celluloid::Actor[device_name] ||= Device.new(device_name)
      Celluloid::Actor[device_name].async.run(Store.new(DB_NAME, TABLE_NAME))
    end
  end
end

Server.run
