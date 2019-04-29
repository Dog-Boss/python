from pathlib import Path
import time
output = {
    "auth":{},
    "acct":{},
}


def t2s(t):
    h, m, s = t.strip().split(":")
    return (int(h) * 3600 + int(m) * 60 + int(s)) % 600


def dictinit(output):
   for key in output.keys():
        for i in range(601):
           output[key][i] = []

def main():
    data_file1 = Path("C:\\Users\\win7\\Desktop\\ora0425_18.dat")
    data1 = data_file1.open("r")

    data_file_dir = Path("C:\\Users\\win7\\Desktop\\")
    hostname = ''
    mes_type = ''
    dictinit(output)
    for line in data1.readlines():
        line = line.strip('\n')
        if line[0] == '#'and line[-1] == '#':
            hostname = line.split('#')[1]
            mes_type = line.split('#')[2]
            if output[mes_type][0].count(hostname) == 0:
                output[mes_type][0].append(hostname)

        else:
            message = line.split(' ')
            timeStamp = t2s(message[1])+1
            output[mes_type][timeStamp].append(int(message[0]))


    for k1,v1 in output.items():
        data2 = Path(data_file_dir,"{}_0425_18.dat".format(k1)).open('w')
        for k2,v2 in v1.items():
            data2.write("{}@{}\n".format(v2,k2))
        data2.close()
    data1.close()


if __name__ == '__main__':
        main()

