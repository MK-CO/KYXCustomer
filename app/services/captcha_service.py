"""
验证码生成服务
"""
import io
import random
import string
from PIL import Image, ImageDraw, ImageFont
from typing import Tuple
import base64
import logging

logger = logging.getLogger(__name__)


class CaptchaService:
    """验证码服务"""
    
    def __init__(self):
        self.width = 120
        self.height = 50
        self.font_size = 24
        self.char_count = 4
        
        # 字符集（去除容易混淆的字符）
        self.chars = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
        
        # 颜色配置
        self.bg_color = (240, 240, 240)
        self.text_colors = [
            (255, 0, 0),    # 红色
            (0, 150, 0),    # 绿色
            (0, 0, 255),    # 蓝色
            (255, 100, 0),  # 橙色
            (128, 0, 128),  # 紫色
        ]
        self.noise_color = (200, 200, 200)
    
    def _generate_text(self) -> str:
        """生成随机验证码文本"""
        return ''.join(random.choices(self.chars, k=self.char_count))
    
    def _add_noise_dots(self, draw: ImageDraw.Draw) -> None:
        """添加噪点"""
        for _ in range(50):
            x = random.randint(0, self.width)
            y = random.randint(0, self.height)
            draw.point((x, y), fill=self.noise_color)
    
    def _add_noise_lines(self, draw: ImageDraw.Draw) -> None:
        """添加干扰线"""
        for _ in range(5):
            x1 = random.randint(0, self.width)
            y1 = random.randint(0, self.height)
            x2 = random.randint(0, self.width)
            y2 = random.randint(0, self.height)
            draw.line([(x1, y1), (x2, y2)], fill=self.noise_color, width=1)
    
    def generate_captcha(self) -> Tuple[str, str]:
        """
        生成验证码图片
        
        Returns:
            Tuple[str, str]: (验证码文本, base64编码的图片)
        """
        try:
            # 生成验证码文本
            captcha_text = self._generate_text()
            
            # 创建图片
            image = Image.new('RGB', (self.width, self.height), self.bg_color)
            draw = ImageDraw.Draw(image)
            
            # 添加噪点
            self._add_noise_dots(draw)
            
            # 绘制文字
            char_width = self.width // self.char_count
            for i, char in enumerate(captcha_text):
                # 随机选择颜色
                color = random.choice(self.text_colors)
                
                # 计算字符位置（添加随机偏移）
                x = i * char_width + random.randint(5, 15)
                y = random.randint(5, 15)
                
                # 尝试使用系统字体，如果失败则使用默认字体
                try:
                    # 这里可以指定字体文件路径，或使用系统默认字体
                    font = ImageFont.load_default()
                except:
                    font = ImageFont.load_default()
                
                draw.text((x, y), char, fill=color, font=font)
            
            # 添加干扰线
            self._add_noise_lines(draw)
            
            # 转换为base64
            img_buffer = io.BytesIO()
            image.save(img_buffer, format='PNG')
            img_buffer.seek(0)
            
            img_base64 = base64.b64encode(img_buffer.getvalue()).decode('utf-8')
            img_data_url = f"data:image/png;base64,{img_base64}"
            
            logger.debug(f"生成验证码: {captcha_text}")
            return captcha_text, img_data_url
            
        except Exception as e:
            logger.error(f"生成验证码失败: {e}")
            # 如果图片生成失败，返回简单的文本验证码
            simple_text = self._generate_text()
            return simple_text, ""


# 全局验证码服务实例
captcha_service = CaptchaService()
