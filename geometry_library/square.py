from abc import ABC, abstractmethod
from math import sqrt, pi, isclose

class Shape(ABC):
    """Абстрактный класс для расчета площади и периметра фигур"""

    @abstractmethod
    def area(self):
        pass

    @abstractmethod
    def perimeter(self):
        pass


class Triangle(Shape):
    """Треугольник по трём сторонам: side_a, side_b, side_c"""

    def __init__(self, side_a: float, side_b: float, side_c: float):
        self.sides = (side_a, side_b, side_c)
        a, b, c = sorted(self.sides)
        if any(side <= 0 for side in self.sides):
            raise ValueError("Стороны должны быть положительными числами")
        if a + b <= c:
            raise ValueError("Нарушено неравенство треугольника")

    def heron(self):
        """Вспомогательный метод: формула Герона"""
        a, b, c = self.sides
        p = sum(self.sides) / 2
        return sqrt(p * (p - a) * (p - b) * (p - c))

    def area(self):
        a, b, c = sorted(self.sides)
        if len(set(self.sides)) == 1:
            # Равносторонний
            return (sqrt(3) * a**2) / 4
        elif len(set(self.sides)) == 2:
            # Равнобедренный
            return self.heron()
        elif isclose(a**2 + b**2, c**2):
            # Прямоугольный
            return (a * b) / 2
        else:
            # Разносторонний
            return self.heron()

    def perimeter(self):
        return sum(self.sides)

    def is_right_triangle(self):
        a, b, c = sorted(self.sides)
        return isclose(a**2 + b**2, c**2)


class Circle(Shape):
    """Круг по radius или diameter"""

    def __init__(self, radius: float = None, diameter: float = None):
        if radius is not None and radius > 0:
            self.radius = radius
        elif diameter is not None and diameter > 0:
            self.radius = diameter / 2
        else:
            raise ValueError("Укажите положительный radius или diameter")

    def area(self):
        return pi * self.radius ** 2

    def perimeter(self):
        return 2 * pi * self.radius
