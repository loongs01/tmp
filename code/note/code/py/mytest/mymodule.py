'''
@Project:mytest
@File:mymodule.py
@IDE:PyCharm
@Author:lichaozhong
@Date:2025/2/8 11:31
'''


# mymodule.py
def func():
    print("func in mymodule.py")


if __name__ == "__main__":
    print("mymodule.py is being run directly")
else:
    print("mymodule.py has been imported into another module")
a = 0
b = 1
while b < 3:
    print(b)
    print(b, end=',')
    # a, b = b, a + b
    a = b
    b = a + b
