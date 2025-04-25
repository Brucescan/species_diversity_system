from multiprocessing import Process, Queue
from fetch_aqi import FetchAQI
from fetch_bird import FetchBird
from save_to_database import consumer



if __name__ == '__main__':
    q = Queue()
    fetch_aqi = FetchAQI()
    fetch_bird = FetchBird()
    p1 = Process(target=fetch_aqi.get_data, args=(q,))
    p2 = Process(target=fetch_bird.get_all_data, args=(q,))
    # 启动第一个生产者进程
    p1.start()
    # 启动第二个生产者进程
    p2.start()

    # 启动消费者
    c = Process(target=consumer, args=(q,))
    c.start()


    


