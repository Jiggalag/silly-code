import random, tkinter


draw_pixel = lambda coords: canvas.create_rectangle(coords[0], coords[1], coords[0] + 1, coords[1] + 1, fill="white", outline="white")
window = tkinter.Tk()
canvas = tkinter.Canvas(window, width=600, height=600, bg="black")
canvas.pack()
tringle = [[random.randint(0, 600), random.randint(0, 600)], [random.randint(0, 600), random.randint(0, 600)], [random.randint(0, 600), random.randint(0, 600)]]
coords = [random.randint(0, 600), random.randint(0, 600)]
for i in range(10000):
    abc = random.randint(0, 2)
    coords = [(coords[0] + tringle[abc][0])/2, (coords[1] + tringle[abc][1])/2]
    draw_pixel(coords)
window.mainloop()