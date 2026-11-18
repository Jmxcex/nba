"""
Pruebas de Calidad de Datos y Validación de Pipeline
Verifica consistencia de esquemas, rango de valores válidos y estructura de capas.
"""

import unittest
import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

class TestNBAPipeline(unittest.TestCase):

    def test_directory_structure(self):
        """Verifica que las carpetas requeridas del proyecto existan"""
        required_dirs = [
            os.path.join(BASE_DIR, "src", "producer"),
            os.path.join(BASE_DIR, "src", "streaming"),
            os.path.join(BASE_DIR, "src", "batch"),
            os.path.join(BASE_DIR, "src", "dashboard"),
            os.path.join(BASE_DIR, "docs")
        ]
        for d in required_dirs:
            self.assertTrue(os.path.exists(d), f"El directorio {d} debe existir.")

    def test_price_range_validations(self):
        """Prueba que la regla de calidad de precios descarte valores fuera del rango (0, 1)"""
        sample_data = pd.DataFrame({
            "market_id": ["m1", "m2", "m3"],
            "price": [-0.5, 0.45, 1.25]
        })
        # Regla aplicada en capa Silver
        valid_prices = sample_data[(sample_data["price"] > 0.0) & (sample_data["price"] < 1.0)]
        self.assertEqual(len(valid_prices), 1)
        self.assertEqual(valid_prices.iloc[0]["price"], 0.45)

    def test_docs_exist(self):
        """Verifica que los documentos de rúbrica estén presentes"""
        required_docs = [
            os.path.join(BASE_DIR, "docs", "OBJETIVOS_Y_METODOLOGIA.md"),
            os.path.join(BASE_DIR, "docs", "ARQUITECTURA_TECNICA.md"),
            os.path.join(BASE_DIR, "docs", "TRAZABILIDAD_IA.md")
        ]
        for doc in required_docs:
            self.assertTrue(os.path.exists(doc), f"El documento de rúbrica {doc} debe existir.")

if __name__ == "__main__":
    unittest.main()
