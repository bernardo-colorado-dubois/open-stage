from src.core.common import Generator, Pipe, Printer, Funnel, Switcher
# Asumiendo que Switcher está en el mismo archivo o importado

# Crear 3 generadores
generator1 = Generator("gen1", 5)
generator2 = Generator("gen2", 5) 
generator3 = Generator("gen3", 5)

# Crear pipes para conectar generadores al funnel
pipe1 = Pipe("pipe1")
pipe2 = Pipe("pipe2")
pipe3 = Pipe("pipe3")

# Crear funnel para combinar los 3 streams
funnel = Funnel("main_funnel")

# Crear pipe del funnel al switcher
pipe_to_switcher = Pipe("pipe_to_switcher")

# Crear switcher que acepta solo números 1 y 2 en campo "number"
switcher = Switcher(
    name="number_switcher",
    field="number", 
    mapping={
        1: "pipe_ones",    # números 1 van a pipe_ones
        2: "pipe_twos"     # números 2 van a pipe_twos
    },
    fail_on_unmatch=False  # ignorar otros números (0, 3, 4, etc.)
)

# Crear pipes de salida del switcher
pipe_ones = Pipe("pipe_ones")
pipe_twos = Pipe("pipe_twos")

# Crear 2 printers
printer_ones = Printer("printer_ones")
printer_twos = Printer("printer_twos")

# Conectar todo el pipeline:
# 3 generadores → funnel → switcher → 2 printers

# Conectar generadores al funnel
generator1.add_output_pipe(pipe1).set_destination(funnel)
generator2.add_output_pipe(pipe2).set_destination(funnel)
generator3.add_output_pipe(pipe3).set_destination(funnel)

# Conectar funnel al switcher
funnel.add_output_pipe(pipe_to_switcher).set_destination(switcher)

# Conectar switcher a los printers
switcher.add_output_pipe(pipe_ones).set_destination(printer_ones)
switcher.add_output_pipe(pipe_twos).set_destination(printer_twos)

# Ejecutar el pipeline
print("=== Iniciando pipeline ===")
print("Ejecutando generadores...")

generator1.pump()
generator2.pump() 
generator3.pump()

print("=== Pipeline completado ===")

# El flujo será:
# 1. Cada generator crea números [0,1,2,3,4]
# 2. Funnel combina los 3 DataFrames en uno solo con 15 filas
# 3. Switcher filtra y envía:
#    - Filas con number=1 → printer_ones
#    - Filas con number=2 → printer_twos
#    - Filas con number=0,3,4 → ignoradas (fail_on_unmatch=False)