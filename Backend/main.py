from flask import Flask, request, jsonify
import oracledb 
import pandas as pd
import os

app = Flask(__name__)


user = "system"
password = "123"
dsn = "localhost/orcl"

oracledb.init_oracle_client()

connection = oracledb.connect(user=user, password=password, dsn=dsn)

@app.route("/cargarmodelo", methods=["POST"])
def carga_archivos():
    carpeta_archivos = request.json["carpeta"]
    if not carpeta_archivos:
        return jsonify({"error": "la carpeta es necesaria"}), 400

    archivos = os.listdir(carpeta_archivos)
    if not archivos:
        return jsonify({"error": "la carpeta esta vacia"}), 400

    extensiones_validas = (".csv")
    cursor = connection.cursor()

    orden_carga = ["paises", "Categorias", "productos", "vendedores", "clientes", "ordenes"]

    for tipo in orden_carga:
        print(tipo)
        for archivo in archivos:
            nombre_archivo, extension = os.path.splitext(archivo)

            if extension in extensiones_validas and nombre_archivo.lower() == tipo.lower():
                with open(os.path.join(carpeta_archivos, archivo), 'r') as file:
                    data = pd.read_csv(file, delimiter=';')

                    if data.empty:
                        return jsonify({"error": "Error al leer el archivo"}), 500
                    if nombre_archivo == "Categorias":
                        for i in data.to_dict(orient='records'):
                            id_categoria = i["id_categoria"]
                            nombre = i["nombre"]
                            query = '''INSERT INTO system.categoria (ID_CATEGORIA, NOMBRE) VALUES (:1, :2)'''
                            cursor.execute(query, (id_categoria, nombre))
                        print("categorias cargadas")
                        connection.commit()
                    elif nombre_archivo == "paises":
                        
                        for i in data.to_dict(orient='records'):
                            
                            id_pais = i["id_pais"]
                            nombre = i["nombre"]
                            query = '''INSERT INTO system.paises(ID_PAIS, NOMBRE) VALUES (:1, :2)'''
                            
                            cursor.execute(query, (id_pais, nombre))

                        connection.commit()
                    elif nombre_archivo == "productos":
                        for i in data.to_dict(orient='records'):
                            id_producto = i["id_producto"]
                            nombre = i["Nombre"]
                            precio = i["Precio"]
                            id_categoria = i["id_categoria"]
                            query = '''INSERT INTO system.producto(ID_PRODUCTO, NOMBRE, PRECIO, ID_CATEGORIA) VALUES (:1, :2, :3, :4)'''
                            cursor.execute(query, (id_producto, nombre, precio, id_categoria))

                        connection.commit()
                    elif nombre_archivo == "vendedores":
                        for i in data.to_dict(orient='records'):
                            id_vendedor = i["id_vendedor"]
                            nombre = i["nombre"]
                            id_pais = i["id_pais"]
                            query = '''INSERT INTO system.vendedores (ID_VENDEDOR, NOMBRE, ID_PAIS) VALUES (:1, :2, :3)'''
                            cursor.execute(query, (id_vendedor, nombre, id_pais))
                            
                        connection.commit()
                    elif nombre_archivo == "clientes":
                        for i in data.to_dict(orient='records'):
                            id_pais = i["id_pais"]
                            id_cliente = i["id_cliente"]
                            nombre = i["Nombre"]
                            apellido = i["Apellido"]
                            direccion = i["Direccion"]
                            tarjeta = i["Tarjeta"]
                            telefono = i["Telefono"]
                            edad = i["Edad"]
                            salario = i["Salario"]
                            genero = i["Genero"]
                            query = '''INSERT INTO system.clientes(ID_CLIENTE, ID_PAIS, NOMBRE, APELLIDO, TELEFONO, TARJETA, EDAD, SALARIO, GENERO, DIRECCION) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)'''
                            cursor.execute(query, (id_cliente, id_pais, nombre, apellido, telefono, tarjeta, edad, salario, genero, direccion))

                        connection.commit()
                    elif nombre_archivo == "ordenes":
                        ordenes_metidas = []
                        for i in data.to_dict(orient='records'):
                            id_orden = i["ï»¿id_orden"]
                            linea_orden = i["linea_orden"]
                            fecha_orden = i["fecha_orden"]
                            id_cliente = i["id_cliente"]
                            id_producto = i["id_producto"]
                            cantidad = i["cantidad"]
                            id_vendedor = i["id_vendedor"]
                            if id_orden not in ordenes_metidas:
                                query = f'''
                                INSERT INTO system.Ordenes (id_orden, fecha_orden, id_cliente) VALUES (:1, TO_DATE(:2, 'DD/MM/YYYY'), :3)
                            '''
                                cursor.execute(query, (id_orden, fecha_orden, id_cliente))
                                connection.commit()
                                ordenes_metidas.append(id_orden)
                            
                            query = f'''
                                INSERT INTO system.Lineas_Orden (id_orden, linea_orden, id_producto, cantidad, id_vendedor) VALUES (:1, :2, :3, :4, :5)
                            '''
                            cursor.execute(query, (id_orden, linea_orden, id_producto, cantidad, id_vendedor))
                            connection.commit()
                            

    cursor.close()

    return jsonify({"message": "Archivos cargados exitosamente"}), 200
@app.route("/borrarinfodb", methods=["GET"])
def eliminar_data_listas():
    try:
        connection = oracledb.connect(user=user, password=password, dsn=dsn)
        cursor = connection.cursor()
        list_tables = ["Lineas_Orden","ORDENES", "CLIENTES", "VENDEDORES", "PRODUCTO", "CATEGORIA", "PAISES"]
        t_eliminadas = []
        for i in list_tables:
            query = f"DELETE FROM system.{i}"
            cursor.execute(query)     
            t_eliminadas.append(i)       
        connection.commit()
        cursor.close()
        return jsonify({"message": "Datos eliminados con exito"}), 200
    except oracledb.DatabaseError as e:
        error_message = "Error de la base de datos: " + str(e)
        return jsonify({"error": error_message}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/eliminarmodelo", methods=["GET"])
def eliminar_modelo():
    try:
        connection = oracledb.connect(user=user, password=password, dsn=dsn)
        cursor = connection.cursor()
        list_tables = ["Lineas_Orden", "ORDENES", "CLIENTES", "VENDEDORES", "PRODUCTO", "CATEGORIA", "PAISES"]
        t_eliminadas = []
        for i in list_tables:
            query = f"DROP TABLE system.{i}"
            cursor.execute(query)
            t_eliminadas.append(i) 
            
        connection.commit()
        cursor.close()
        return jsonify({"message": "Tablas eliminadas con exito"}), 200
    except oracledb.DatabaseError as e:
        error_message = "Error de la base de datos: " + str(e)
        return jsonify({"error": error_message}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/crearmodelo", methods=["POST"])
def crear_Script():
    try:
        carpeta_archivos = request.json.get("carpeta")
        if not carpeta_archivos:
            return jsonify({"error": "La carpeta de archivos es necesaria"}), 400
        
        script_file = os.path.join(carpeta_archivos, "BTbase.sql")
        if not os.path.exists(script_file):
            return jsonify({"error": "El archivo BTbase.sql es necesario"}), 400

        with open(script_file, 'r') as file:
            sql_script = file.read()

        cursor = connection.cursor()
        for query in sql_script.split(';'):
            try:
                query = query.strip()
                if query:
                    cursor.execute(query)
            except oracledb.DatabaseError as e:
                error_message = "Error de base de datos: " + str(e)
                connection.rollback()
                return jsonify({"error": error_message}), 500

        connection.commit()
        cursor.close()

        return jsonify({"message": "Script ejecutado exitosamente"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/consulta1", methods=["GET"])
def consulta1():
    try:
        cursor = connection.cursor()
        query = '''
            SELECT  
                system.Clientes.id_cliente,
                system.Clientes.Nombre,
                system.Clientes.Apellido,
                system.Paises.nombre AS Pais,
            SUM(system.Lineas_Orden.cantidad * system.Producto.Precio) AS Monto_Total
            FROM Clientes
                JOIN system.Ordenes ON system.Clientes.id_cliente = system.Ordenes.id_cliente
                JOIN system.Lineas_Orden ON system.Ordenes.id_orden = system.Lineas_Orden.id_orden
                JOIN system.Producto ON system.Lineas_Orden.id_producto = system.Producto.id_producto
                JOIN system.Vendedores ON system.Lineas_Orden.id_vendedor = system.Vendedores.id_vendedor
                JOIN system.Paises ON system.Clientes.id_pais = system.Paises.id_pais
            GROUP BY 
                    system.Clientes.id_cliente,
                    system.Clientes.Nombre,
                    system.Clientes.Apellido,
                    system.Paises.nombre

            ORDER BY Monto_Total DESC 
            FETCH FIRST 1 ROW ONLY
            '''
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        detalles = []
        for row in data:
            detalle = {
                "id_cliente": row[0],
                "nombre": row[1],
                "apellido": row[2],
                "pais": row[3],
                "monto_total": row[4]
            } 
            detalles.append(detalle)

        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route("/consulta2", methods=["GET"])
def consulta2():
    try:
        cursor = connection.cursor()
        
        query_mas_comprado = '''
            SELECT 
                Producto.id_producto,
                Producto.nombre AS nombre_producto,
                Categoria.nombre AS categoria,
                SUM(Lineas_Orden.cantidad) AS cantidad_unidades,
                SUM(Lineas_Orden.cantidad * Producto.Precio) AS monto_vendido
            FROM Lineas_Orden
            JOIN Producto ON Lineas_Orden.id_producto = Producto.id_producto
            JOIN Categoria ON Producto.id_categoria = Categoria.id_categoria
            GROUP BY 
                Producto.id_producto,
                Producto.nombre,
                Categoria.nombre
            ORDER BY 
                cantidad_unidades DESC
            FETCH FIRST 1 ROW ONLY
        '''
        
        cursor.execute(query_mas_comprado)
        data_mas_comprado = cursor.fetchall()
        
        query_menos_comprado = '''
            SELECT 
                Producto.id_producto,
                Producto.nombre AS nombre_producto,
                Categoria.nombre AS categoria,
                SUM(Lineas_Orden.cantidad) AS cantidad_unidades,
                SUM(Lineas_Orden.cantidad * Producto.Precio) AS monto_vendido
            FROM Lineas_Orden
            JOIN Producto ON Lineas_Orden.id_producto = Producto.id_producto
            JOIN Categoria ON Producto.id_categoria = Categoria.id_categoria
            GROUP BY 
                Producto.id_producto,
                Producto.nombre,
                Categoria.nombre
            ORDER BY 
                cantidad_unidades ASC
            FETCH FIRST 1 ROW ONLY
        '''
        
        cursor.execute(query_menos_comprado)
        data_menos_comprado = cursor.fetchall()
        
        cursor.close()
        
        detalles = {
            "producto_mas_comprado": [],
            "producto_menos_comprado": []
        }
        
        for row in data_mas_comprado:
            detalle = {
                "id": row[0],
                "nombre": row[1],
                "categoria": row[2],
                "cantidad_unidades": row[3],
                "monto_vendido": row[4]
            } 
            detalles["producto_mas_comprado"].append(detalle)
            
        for row in data_menos_comprado:
            detalle = {
                "id": row[0],
                "nombre": row[1],
                "categoria": row[2],
                "cantidad_unidades": row[3],
                "monto_vendido": row[4]
            } 
            detalles["producto_menos_comprado"].append(detalle)

        return jsonify(detalles), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route("/consulta3", methods=["GET"])
def consulta3():
    try:
        cursor = connection.cursor()
        query = '''
            SELECT 
                Vendedores.id_vendedor,
                Vendedores.Nombre AS nombre_vendedor,
                SUM(Lineas_Orden.cantidad * Producto.Precio) AS monto_total_vendido
            FROM Lineas_Orden
            JOIN Vendedores ON Lineas_Orden.id_vendedor = Vendedores.id_vendedor
            JOIN Producto ON Lineas_Orden.id_producto = Producto.id_producto
            GROUP BY 
                Vendedores.id_vendedor,
                Vendedores.Nombre
            ORDER BY 
                monto_total_vendido DESC
            FETCH FIRST 1 ROW ONLY
            '''
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        detalles = []
        for row in data:
            detalle = {
                "id_Vendedor": row[0],
                "nombre_vendedor": row[1],
                "Total_vendido": row[2],
            } 
            detalles.append(detalle)

        return jsonify(data), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route("/consulta4", methods=["GET"])
def consulta4():
    try:
        cursor = connection.cursor()

        
        cursor.execute(query_pais_menos_vendido)
        data_pais_mas_vendido = cursor.fetchall()
        
        query_pais_menos_vendido = '''
            SELECT 
                Paises.nombre AS nombre_pais,
                SUM(Lineas_Orden.cantidad * Producto.Precio) AS monto_total_vendido
            FROM 
                Lineas_Orden
            JOIN  
                Vendedores ON Lineas_Orden.id_vendedor = Vendedores.id_vendedor
            JOIN  
                Paises ON Vendedores.id_pais = Paises.id_pais
            JOIN  
                Producto ON Lineas_Orden.id_producto = Producto.id_producto
            GROUP BY 
                Paises.nombre
            ORDER BY 
                monto_total_vendido ASC
            FETCH FIRST 1 ROW ONLY
        '''

        cursor.execute(query_pais_mas_vendido)
        data_pais_menos_vendido = cursor.fetchall()
        
        cursor.close()
        
        detalles = {
            "pais_mas_vendido": [],
            "pais_menos_vendido": []
        }
        
        for row in data_pais_mas_vendido:
            detalle = {
                "nombre_pais": row[0],
                "monto_total_vendido": row[1]
            } 
            detalles["pais_menos_vendido"].append(detalle)
            
        for row in data_pais_menos_vendido:
            detalle = {
                "nombre_pais": row[0],
                "monto_total_vendido": row[1]
            } 
            detalles["pais_mas_vendido"].append(detalle)

        return jsonify(detalles), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route("/consulta5", methods=["GET"])
def consulta5():
    try:
        cursor = connection.cursor()
        query = '''
            SELECT 
                Paises.id_pais,
                Paises.nombre AS nombre_pais,
                SUM(Lineas_Orden.cantidad * Producto.Precio) AS monto_total
            FROM 
                Lineas_Orden
            JOIN Producto ON Lineas_Orden.id_producto = Producto.id_producto
            JOIN Ordenes ON Lineas_Orden.id_orden = Ordenes.id_orden
            JOIN Clientes ON Ordenes.id_cliente = Clientes.id_cliente
            JOIN Paises ON Clientes.id_pais = Paises.id_pais
            GROUP BY 
                Paises.id_pais,
                Paises.nombre
            ORDER BY 
                monto_total ASC
        '''
        cursor.execute(query)
        data = cursor.fetchmany(5) 
        cursor.close()
        
        detalles = []
        for row in data:
            detalle = {
                "id_pais": row[0],
                "nombre_pais": row[1],
                "monto_total": row[2]
            } 
            detalles.append(detalle)

        return jsonify(detalles), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route("/consulta6", methods=["GET"])
def consulta6():
    try:
        cursor = connection.cursor()

        query_mas_comprada = '''
            SELECT 
                Categoria.nombre AS nombre_categoria,
                SUM(Lineas_Orden.cantidad) AS cantidad_unidades
            FROM 
                Lineas_Orden
            JOIN 
                Producto ON Lineas_Orden.id_producto = Producto.id_producto
            JOIN 
                Categoria ON Producto.id_categoria = Categoria.id_categoria
            GROUP BY 
                Categoria.nombre
            ORDER BY 
                cantidad_unidades DESC
            FETCH FIRST 1 ROW ONLY
        '''
        cursor.execute(query_mas_comprada)
        data_mas_comprada = cursor.fetchall()
        
        query_menos_comprada = '''
            SELECT 
                Categoria.nombre AS nombre_categoria,
                SUM(Lineas_Orden.cantidad) AS cantidad_unidades
            FROM 
                Lineas_Orden
            JOIN 
                Producto ON Lineas_Orden.id_producto = Producto.id_producto
            JOIN 
                Categoria ON Producto.id_categoria = Categoria.id_categoria
            GROUP BY 
                Categoria.nombre
            ORDER BY 
                cantidad_unidades ASC
            FETCH FIRST 1 ROW ONLY
        '''
        cursor.execute(query_menos_comprada)
        data_menos_comprada = cursor.fetchall()
        
        cursor.close()
        
        detalles = {
            "categoria_mas_comprada": [],
            "categoria_menos_comprada": []
        }
        
        for row in data_mas_comprada:
            detalle = {
                "nombre_categoria": row[0],
                "cantidad_unidades": row[1]
            } 
            detalles["categoria_mas_comprada"].append(detalle)
            
        for row in data_menos_comprada:
            detalle = {
                "nombre_categoria": row[0],
                "cantidad_unidades": row[1]
            } 
            detalles["categoria_menos_comprada"].append(detalle)

        return jsonify(detalles), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route("/consulta7", methods=["GET"])
def consulta7():
    try:
        cursor = connection.cursor()
        query = '''
            with t_aux as (SELECT  
                    p.nombre as nombre_pais,
                    ca.nombre as nombre_categoria,
                    sum(l.cantidad)  as cantidad_total,
            ROW_NUMBER() OVER (PARTITION BY p.nombre ORDER BY sum(l.cantidad) DESC) AS fila_numero
            FROM paises p
                JOIN Clientes c on p.id_pais = c.id_pais
                JOIN ordenes o on o.id_cliente = c.id_cliente
                JOIN lineas_orden l on l.id_orden = o.id_orden
                JOIN producto pu on pu.id_producto = l.id_producto
                JOIN categoria ca on pu.id_categoria = ca.id_categoria
            GROUP BY p.nombre, ca.nombre)
            select nombre_pais, nombre_categoria, cantidad_total  from t_aux where fila_numero = 1 order by cantidad_total desc
        '''
        cursor.execute(query)
        data = cursor.fetchall()
        
        cursor.close()
        
        detalles = []
        
        for row in data:
            detalle = {
                "nombre_pais": row[0],
                "nombre_categoria": row[1],
                "cantidad_unidades": row[2]
            } 
            detalles.append(detalle)

        return jsonify(detalles), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route("/consulta8", methods=["GET"])
def consulta8():
    try:
        cursor = connection.cursor()
        query = '''
        SELECT
            EXTRACT(MONTH FROM fecha_orden) AS numero_mes,
            SUM(Lineas_Orden.cantidad * Producto.Precio) AS monto
        FROM
            Ordenes
        INNER JOIN Lineas_Orden ON Ordenes.id_orden = Lineas_Orden.id_orden
        INNER JOIN Producto ON Lineas_Orden.id_producto = Producto.id_producto
        INNER JOIN Vendedores ON Lineas_Orden.id_vendedor = Vendedores.id_vendedor
        INNER JOIN Paises ON Vendedores.id_pais = Paises.id_pais
        WHERE
            Paises.nombre = 'Inglaterra'
        GROUP BY
            EXTRACT(MONTH FROM fecha_orden)
        '''
        cursor.execute(query)
        data = cursor.fetchall()

        cursor.close()
        
        detalles = []

        for row in data:
            detalle = {
                "numero_mes": row[0],
                "monto": row[1]
            } 
            detalles.append(detalle)

        return jsonify(detalles), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route("/consulta9", methods=["GET"])
def consulta9():
    try:
        cursor = connection.cursor()

        query = '''
            SELECT 
                TO_CHAR(Ordenes.fecha_orden, 'MM') AS numero_mes,
                SUM(Lineas_Orden.cantidad * Producto.Precio) AS monto
            FROM 
                Ordenes
            JOIN 
                Lineas_Orden ON Ordenes.id_orden = Lineas_Orden.id_orden
            JOIN 
                Producto ON Lineas_Orden.id_producto = Producto.id_producto
            GROUP BY 
                TO_CHAR(Ordenes.fecha_orden, 'MM')
            ORDER BY 
                SUM(Lineas_Orden.cantidad * Producto.Precio) DESC
        '''
        cursor.execute(query)
        data = cursor.fetchall()

        cursor.close()
        
        mes_mas_ventas = data[0] if data else None
        
        mes_menos_ventas = data[-1] if data else None

        response = {
            "mes_con_mas_ventas": {
                "numero_mes": mes_mas_ventas[0],
                "monto": mes_mas_ventas[1] if mes_mas_ventas else None
            },
            "mes_con_menos_ventas": {
                "numero_mes": mes_menos_ventas[0],
                "monto": mes_menos_ventas[1] if mes_menos_ventas else None
            }
        }

        return jsonify(response), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route("/consulta10", methods=["GET"])
def consulta10():
    try:
        cursor = connection.cursor()

        query = '''
            SELECT 
                Producto.id_producto,
                Producto.nombre AS nombre_producto,
                SUM(Lineas_Orden.cantidad * Producto.Precio) AS monto
            FROM 
                Producto
            JOIN 
                Categoria ON Producto.id_categoria = Categoria.id_categoria
            JOIN 
                Lineas_Orden ON Producto.id_producto = Lineas_Orden.id_producto
            WHERE 
                Categoria.nombre = 'Deportes'
            GROUP BY 
                Producto.id_producto,
                Producto.nombre
            ORDER BY 
                Producto.id_producto
        '''
        cursor.execute(query)
        data = cursor.fetchall()

        cursor.close()
        
        detalles = []

        for row in data:
            detalle = {
                "id_producto": row[0],
                "nombre_producto": row[1],
                "monto": row[2]
            } 
            detalles.append(detalle)

        return jsonify(detalles), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
