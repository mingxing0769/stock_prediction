import random
import argparse
from common import *
from multiprocessing.pool import ThreadPool   

parser = argparse.ArgumentParser()
parser.add_argument('--pklname', default="train.pkl", type=str, help="code")
args = parser.parse_args()



def load_data(csv_file):
    data = pd.read_csv(csv_file)
    data = data.dropna()
    data.fillna(0, inplace=True) 
    ts_code = os.path.basename(csv_file).split(".")[0]
    data["ts_code"] = ts_code
    data_queue.put(data)
    
def process_data():
    while data_queue.empty() == False:
        try:
            data = data_queue.get(timeout=1)  
            if data.empty or data["ts_code"][0] == "None":
                print("data is empty or data has invalid col")  
                continue         
            ts_code = data["ts_code"][0]
            dump_queue.put(data)                
        except Exception as e:
            print(ts_code, e)
            continue  

if __name__ == ("__main__"):
    csv_files = glob.glob(daily_path+"/*.csv")
    # data_list = []
    ts_codes =[] 
    Train_data = pd.DataFrame()
    data_len = 0
    dump_queue=queue.Queue()
    for csv_file in csv_files:
        ts_codes.append(os.path.basename(csv_file).rsplit(".", 1)[0])
    random.shuffle(ts_codes)
    
    # 创建线程池,最大线程数设置为CPU核心数
    pool = ThreadPool(multiprocessing.cpu_count())     
    
    # 遍历所有CSV文件,使用线程池启动线程读取 
    for csv_file in csv_files: 
        pool.apply_async(load_data, args=(csv_file,))  
    
    # 启动多个线程从data_queue队列获取数据并处理
    for _ in range(10):  
        pool.apply_async(process_data, args=())
        
    # 关闭线程池
    pool.close()  
    #等待所有线程结束  
    pool.join()      
    
    with open(pkl_path+"/"+args.pklname, "wb") as f:
        dill.dump(dump_queue, f)
    print("dump_queue size: ", dump_queue.qsize())
