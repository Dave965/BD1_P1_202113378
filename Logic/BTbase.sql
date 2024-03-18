CREATE TABLE Paises (
    id_pais INTEGER PRIMARY KEY,
    nombre VARCHAR2(100) NOT NULL
);

CREATE TABLE Categoria (
    id_categoria INTEGER PRIMARY KEY,
    nombre VARCHAR2(100) NOT NULL
);

CREATE TABLE Producto (
    id_producto INTEGER PRIMARY KEY,
    nombre VARCHAR2(300) NOT NULL,
    Precio FLOAT NOT NULL,
    id_categoria INTEGER NOT NULL,
    FOREIGN KEY (id_categoria) REFERENCES Categoria(id_categoria)
);

CREATE TABLE Vendedores (
    id_vendedor INTEGER PRIMARY KEY,
    id_pais INTEGER NOT NULL,
    Nombre VARCHAR2(300) NOT NULL,
    FOREIGN KEY (id_pais) REFERENCES Paises(id_pais)
);

CREATE TABLE Clientes (
    id_cliente INTEGER PRIMARY KEY,
    id_pais INTEGER NOT NULL,
    Nombre VARCHAR2(100) NOT NULL,
    Apellido VARCHAR2(100) NOT NULL,
    Telefono INTEGER NOT NULL,
    Tarjeta NUMBER(16) NOT NULL,
    Edad INTEGER NOT NULL,
    salario INTEGER NOT NULL,
    Genero VARCHAR(1) NOT NULL,
    Direccion VARCHAR2(100) NOT NULL,
    FOREIGN KEY (id_pais) REFERENCES Paises(id_pais)
);

CREATE TABLE Ordenes (
    id_orden INTEGER PRIMARY KEY,
    fecha_orden DATE NOT NULL,
    id_cliente INTEGER NOT NULL,
    FOREIGN KEY (id_cliente) REFERENCES Clientes(id_cliente)
);

CREATE TABLE Lineas_Orden (
    id_orden INTEGER NOT NULL,
    linea_orden INTEGER NOT NULL,
    id_producto INTEGER NOT NULL,
    cantidad INTEGER NOT NULL,
    id_vendedor INTEGER NOT NULL,
    primary key(id_orden, linea_orden),
    FOREIGN KEY (id_orden) REFERENCES Ordenes(id_orden),
    FOREIGN KEY (id_producto) REFERENCES Producto(id_producto),
    FOREIGN KEY (id_vendedor) REFERENCES Vendedores(id_vendedor)

);

-- Eliminar las tablas
DROP TABLE Ordenes;
DROP TABLE Clientes;
DROP TABLE Vendedores;
DROP TABLE Producto;
DROP TABLE Categoria;
DROP TABLE Paises;
DROP TABLE Lineas_Orden
--INSERT INTO system.Ordenes (id_orden, fecha_orden, id_cliente) VALUES (1, TO_DATE('27/01/2004', 'DD/MM/YYYY'), 7888);
--DROP TABLE Venta;
--DROP TABLE Producto;
--DROP TABLE Telefono;
--DROP TABLE Categoria;
--DROP TABLE Cliente;
--DROP TABLE Proveedor;
--delete from producto
--delete from vendedores
--delete from clientes
--INSERT INTO system.vendedores (ID_VENDEDOR, ID_PAIS, NOMBRE) VALUES (1, 1, 'andy')
--CONSULTAS PROGRAMADAS 
SELECT  
    Clientes.id_cliente,
    Clientes.Nombre,
    Clientes.Apellido,
    Paises.nombre AS Pais,
    SUM(Lineas_Orden.cantidad * Producto.Precio) AS Monto_Total
FROM Clientes
JOIN Ordenes ON Clientes.id_cliente = Ordenes.id_cliente
JOIN Lineas_Orden ON Ordenes.id_orden = Lineas_Orden.id_orden
JOIN Producto ON Lineas_Orden.id_producto = Producto.id_producto
JOIN Vendedores ON Lineas_Orden.id_vendedor = Vendedores.id_vendedor
JOIN Paises ON Clientes.id_pais = Paises.id_pais
GROUP BY 
    Clientes.id_cliente,
    Clientes.Nombre,
    Clientes.Apellido,
    Paises.nombre

ORDER BY Monto_Total DESC 
    --LIMIT1 NO FUNCIONA ORA-00933 es especifico de MYSQL
    --utilizamos fetch first 1 row only
FETCH FIRST 1 ROW ONLY;

--=============2
SELECT Producto.id_producto,
    Producto.Nombre AS nombre_producto