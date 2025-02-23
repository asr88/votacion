from flask import Flask, request, jsonify
from flask_restful import Resource, Api
import requests
from typing import List, Dict
import concurrent.futures

app = Flask(__name__)
api = Api(app)

class VistaVerificador(Resource):
    def __init__(self):
        # Lista de URLs donde está desplegado el servicio de procesar ventas
        self.endpoints = [
            "http://localhost:5000/procesar_venta",
            "http://localhost:5001/procesar_venta",
            "http://localhost:5002/procesar_venta"
        ]
        # Verificar que el número de endpoints sea impar
        if len(self.endpoints) % 2 == 0:
            raise ValueError("El número de endpoints debe ser impar")

    def hacer_peticion(self, url: str, datos: Dict) -> Dict:
        """Realiza una petición individual y maneja posibles errores"""
        try:
            response = requests.post(url, json=datos)
            return {
                'url': url,
                'status_code': response.status_code,
                'resultado': response.json() if response.status_code == 200 else None,
                'error': None
            }
        except Exception as e:
            return {
                'url': url,
                'status_code': 500,
                'resultado': None,
                'error': str(e)
            }

    def verificar_resultados(self, resultados: List[Dict]) -> Dict:
        """Verifica si los resultados son consistentes entre sí"""
        # Filtrar solo resultados exitosos
        resultados_exitosos = [r for r in resultados if r['status_code'] == 200]
        
        if not resultados_exitosos:
            return {
                'consistente': False,
                'mensaje': "No se obtuvieron resultados exitosos",
                'resultados_diferentes': []
            }

        # Extraer los totales de venta y contar frecuencias
        totales = [r['resultado']['total_venta'] for r in resultados_exitosos]
        frecuencias = {}
        for r in resultados_exitosos:
            total = r['resultado']['total_venta']
            frecuencias[total] = frecuencias.get(total, 0) + 1

        # Si hay un solo valor, todos son iguales
        if len(frecuencias) == 1:
            return {
                'consistente': True,
                'mensaje': "Todos los resultados son consistentes",
                'resultados_diferentes': []
            }

        # Si hay más de un valor diferente
        if len(frecuencias) == len(resultados_exitosos):
            return {
                'consistente': False,
                'mensaje': "Todos los resultados son diferentes entre sí",
                'resultados_diferentes': [
                    {'url': r['url'], 'total': r['resultado']['total_venta']}
                    for r in resultados_exitosos
                ]
            }

        # Encontrar el valor minoritario (el diferente)
        valor_mayoritario = max(frecuencias.items(), key=lambda x: x[1])[0]
        
        # Identificar los resultados diferentes (los que no son mayoritarios)
        resultados_diferentes = [
            {
                'url': r['url'],
                'total': r['resultado']['total_venta']
            }
            for r in resultados_exitosos
            if r['resultado']['total_venta'] != valor_mayoritario
        ]

        return {
            'consistente': False,
            'mensaje': "Se encontraron diferencias en los resultados",
            'resultados_diferentes': resultados_diferentes
        }

    def post(self):
        data = request.get_json()
        
        if 'cantidades' not in data or 'precios' not in data:
            return {'error': 'Faltan parámetros: cantidades y precios son requeridos.'}, 400

        # Realizar peticiones en paralelo
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.hacer_peticion, url, data)
                for url in self.endpoints
            ]
            resultados = [f.result() for f in futures]

        # Verificar consistencia de resultados
        verificacion = self.verificar_resultados(resultados)
        
        return {
            'resultados_individuales': resultados,
            'verificacion': verificacion
        }, 200
        
api.add_resource(VistaVerificador, '/verificar')