import serial

class temp_sensor:

    def __init__ (self, dev, port):
        self.ser = serial.Serial(dev, port)

    def read_line ():
        return self.ser.readline()



if __name__ == "__main__":
    temp_sens = temp_sensor("/dev/ttyACM1", 9600)
    while True:
        print temp_sens.read_line()