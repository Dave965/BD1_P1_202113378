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
