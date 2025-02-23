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
            "http://localhost:5000/procesar_venta"
        ]

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

        # Extraer los totales de venta para comparar
        totales = [r['resultado']['total_venta'] for r in resultados_exitosos]
        primer_total = totales[0]
        
        # Verificar si todos los resultados son iguales
        resultados_diferentes = [
            {
                'url': r['url'],
                'total': r['resultado']['total_venta']
            }
            for r in resultados_exitosos
            if r['resultado']['total_venta'] != primer_total
        ]

        return {
            'consistente': len(resultados_diferentes) == 0,
            'mensaje': "Todos los resultados son consistentes" if len(resultados_diferentes) == 0 
                      else "Se encontraron diferencias en los resultados",
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