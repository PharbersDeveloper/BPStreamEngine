# from py4j.java_gateway import JavaGateway
#
# if __name__ == "__main__":
#     gateway = JavaGateway()  # connect to the JVM
#     random = gateway.jvm.java.util.Random()  # create a java.util.Random instance
#     number1 = random.nextInt(10)  # call the Random.nextInt method
#     number2 = random.nextInt(10)
#     print(number1, number2)
#     addition_app = gateway.entry_point  # get the AdditionApplication instance
#     value = addition_app.addition(number1, number2)  # call the addition method
#     print(value)

# import numpy
import sys

if __name__ == '__main__':
    a= 1
    print(a)
    print(sys.argv)
    print("hello,this is python")
