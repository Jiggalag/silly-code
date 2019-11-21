import matplotlib.pyplot as plt
import random
fig = plt.figure()
xa = 0.0
ya = 0.0
xb = 1.0
yb = 0.5
xc = 0.0
yc = 1.0
x = 0.0
y = 0.5
scatter1 = plt.scatter(xa, ya)
scatter2 = plt.scatter(xb, yb)
scatter3 = plt.scatter(xc, yc)
for i in range(0, 1000):
    dice = random.randint(1, 6)
    if dice <= 2:
        x = (x + xa)/2
        y = (y + ya)/2
    elif dice >= 5:
        x = (x + xb)/2
        y = (y + yb)/2
    else:
        x = (x + xc)/2
        y = (y + yc)/2
    scatterx = plt.scatter(x, y)
plt.show()