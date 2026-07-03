import unittest
import fakeredis
import math
import sys
import os

# プロジェクトルートをパスに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.csv2redis import Csv2redisClass

class TestSpecialCharacterPatterns(unittest.TestCase):
    def setUp(self):
        # Redisのモックを作成
        self.f_redis = fakeredis.FakeStrictRedis(version=6)
        self.c2r = Csv2redisClass()
        self.c2r.set_connect(self.f_redis)
        self.c2r.init("test_special:")
        
        # テスト用のスキーマ設定（id, name, latitude, longitude, note）
        self.c2r.schemaObj = {
            'schema': ['id', 'name', 'latitude', 'longitude', 'note'],
            'type': [Csv2redisClass.T_STR, Csv2redisClass.T_STR, Csv2redisClass.T_NUMB, Csv2redisClass.T_NUMB, Csv2redisClass.T_STR],
            'latCol': 2,
            'lngCol': 3,
            'titleCol': 1,
            'idCol': -1,
            'namespace': 'test_special:',
            'name': 'test_layer'
        }

    def tearDown(self):
        self.f_redis.close()

    def run_add_delete_test(self, test_data_list):
        """登録して削除できるかの一連の流れをテストするヘルパー"""
        max_level = 16
        # 1. 登録
        reg_result = self.c2r.burstRegistData(test_data_list, max_level)
        self.assertGreaterEqual(reg_result['success'], 1, "登録に失敗しました")
        
        # 2. 削除
        del_result_dict = self.c2r.burstDeleteData(test_data_list, max_level)
        # 削除成功数を確認
        self.assertEqual(del_result_dict["success"], len(test_data_list), f"削除に失敗しました。戻り値: {del_result_dict}")

    def test_half_width_pattern(self):
        """半角英数字のパターン"""
        data = [{
            'lat': 35.0, 'lng': 135.0,
            'hkey': '3500000:13500000:ID001,AlphaName,Note1',
            'data': 'ID001,AlphaName,35.0,135.0,Note1'
        }]
        self.run_add_delete_test(data)

    def test_full_width_pattern(self):
        """全角日本語のパターン"""
        data = [{
            'lat': 35.1, 'lng': 135.1,
            'hkey': '3510000:13510000:ID002,日本語名,備考データ',
            'data': 'ID002,日本語名,35.1,135.1,備考データ'
        }]
        self.run_add_delete_test(data)

    def test_full_width_hyphen(self):
        """全角ハイフンのパターン"""
        data = [{
            'lat': 35.3, 'lng': 135.3,
            'hkey': '3530000:13530000:ID－004,ハイフン－あり,備考－全角',
            'data': 'ID－004,ハイフン－あり,35.3,135.3,備考－全角'
        }]
        self.run_add_delete_test(data)

    def test_full_width_space(self):
        """全角スペースのパターン"""
        data = [{
            'lat': 35.4, 'lng': 135.4,
            'hkey': '3540000:13540000:ID005,名称　太郎,備考　全角スペース',
            'data': 'ID005,名称　太郎,35.4,135.4,備考　全角スペース'
        }]
        self.run_add_delete_test(data)

    def test_full_width_symbols(self):
        """全角記号（カンマ、コロン）のパターン"""
        data = [{
            'lat': 35.2, 'lng': 135.2,
            'hkey': '3520000:13520000:ID003,名称：テスト,備考，カンマあり',
            'data': 'ID003,名称：テスト,35.2,135.2,備考，カンマあり'
        }]
        self.run_add_delete_test(data)

if __name__ == '__main__':
    unittest.main()
