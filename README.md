# DataEngineeringWithKakfa

## Setting up the Project
```bash
git clone https://github.com/Yokeshthirumoorthi/DataEngineeringWithKakfa.git
cd DataEngineeringWithKakfa   
```

## Setting up the VM

```bash
sudo apt-get update
sudo apt-get install python3-pip virtualenv git
virtualenv confluent-exercise --python=python3
```
## How to build

```bash
source ./confluent-exercise/bin/activate
pip3 install confluent-kafka
pip3 install -r requirements.txt
```