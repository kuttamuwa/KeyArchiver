import threading


def logic_one():
    print("l1")


def logic_two():
    print("l2")


t1 = threading.Thread(name="t1", target=logic_one)
t2 = threading.Thread(name="t2", target=logic_two)
t1.start()
t2.start()
