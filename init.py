import multiprocessing
import os
import queue
import torch
import threading
import copy
import torch.nn as nn
import torch.optim as optim
import pandas as pd
from torch.cuda.amp import autocast, GradScaler
from torch.utils.data import Dataset, DataLoader
from tqdm import tqdm

TRAIN_WEIGHT=0.9
SEQ_LEN=30
LEARNING_RATE=0.00001   # 0.00001
WEIGHT_DECAY=0.05   # 0.05
BATCH_SIZE=4
EPOCH=1
SAVE_NUM_ITER=10
SAVE_NUM_EPOCH=1
# GET_DATA=True
TEST_NUM=25
SAVE_INTERVAL=300
OUTPUT_DIMENSION=8
INPUT_DIMENSION=20
TQDM_NCOLS = 150
NUM_WORKERS = 1
PKL = True
BUFFER_SIZE = 10

symbol = 'Generic.Data'
# symbol = '000001.SZ'
cnname = ""
for item in symbol.split("."):
    cnname += item
lstm_path="./"+cnname+"/LSTM"
transformer_path="./"+cnname+"/TRANSFORMER"
save_path=lstm_path

loss_list=[]
data_list=[]
mean_list=[]
std_list=[]
safe_save = False
data_queue=multiprocessing.Queue()
stock_data_queue=queue.Queue()
stock_list_queue = queue.Queue()
csv_queue=queue.Queue()
df_queue=queue.Queue()

NoneDataFrame = pd.DataFrame(columns=["ts_code"])
NoneDataFrame["ts_code"] = ["None"]

name_list = ["open", "high", "low", "close", "change", "pct_chg", "vol", "amount"]
use_list = [1,1,1,1,1,1,1,1]
OUTPUT_DIMENSION = sum(use_list)

assert OUTPUT_DIMENSION > 0

device=torch.device("cuda" if torch.cuda.is_available() else "cpu")
if device.type == "cuda":
    torch.backends.cudnn.benchmark = True

def check_exist(address):
    if os.path.exists(address) == False:
        os.mkdir(address)

train_path="./stock_handle/stock_train.csv"
test_path="./stock_handle/stock_test.csv"
train_pkl_path="./pkl_handle/train.pkl"
png_path="./png"
daily_path="./stock_daily"
handle_path="./stock_handle"
pkl_path="./pkl_handle"

check_exist("./" + cnname)
check_exist(handle_path)
check_exist(daily_path)
check_exist(pkl_path)
check_exist(png_path)
check_exist(png_path+"/train_loss/")
check_exist(png_path+"/predict/")
check_exist(png_path+"/test/")