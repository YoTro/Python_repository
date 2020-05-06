#! /bin/bash
# 用法：./pasTrig [个数]，若不指明个数为 5。
# 填充指定个数的空格
pad(){ for ((k=0;k<$1;k++)); do echo -n ' '; done; }
# 层数和新旧层
lyrs=${1-10}
prev[0]=1
curr[0]=1 # 接下来每行第一个始终为一，无需重复赋值
# 执行
pad $(((lyrs-1)*2))
echo 1
for ((i=2; i<=lyrs; i++)); do # 略过 1，已处理
  pad $(((lyrs-i)*2)) # 填充空格，注意这里不会怎么顾及三位以上的数，即第 14 层开始会混乱
  curr[i]=1
  printf '%-4d' ${curr[0]}
  for ((j=1; j<i-1; j++)); do # 首尾极值已处理，略过
    ((curr[j]=prev[j-1]+prev[j]))
    printf '%-4d' ${curr[j]}
  done
  printf '%-4d\n' ${curr[i]} # 最后一个和换行
  # 搬家
    prev=(${curr[*]})
    done