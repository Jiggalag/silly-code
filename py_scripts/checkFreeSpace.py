import subprocess

user = 'mock'
server = 'mock'
threshold = 90
pathToImage = '/home/jiggalag/fffuuu.jpg'
getFreeSpace = "ssh %s@%s 'df -h | grep /dev/sdb1'" % (user, server)
freeSpace = subprocess.check_output(getFreeSpace, shell=True, universal_newlines=True)
percent = freeSpace[freeSpace.find('%') - 2:freeSpace.find('%')]
if int(percent) >= threshold:
    openImage = 'xdg-open %s' % pathToImage
    subprocess.call(openImage, shell=True)
