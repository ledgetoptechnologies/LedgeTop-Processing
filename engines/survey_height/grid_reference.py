"""Independent height and projection reference. Pure Python: no PROJ, no GDAL.

The conversion under test uses PROJ. Every expected value in the gate comes from
here instead: the pinned GEOID18 GeoTIFF is decoded directly and interpolated
bilinearly, and the transverse Mercator step is the Krueger series.
"""
import hashlib
import math
import struct
import zlib
from pathlib import Path

GRID_NAME = 'us_noaa_g2018u0.tif'
GRID_SHA256 = 'fa9a407ac7ee3f5a3694008e4bcd09ce9cc250452f0c3b11700a4960340abce2'
GRID_SIZE = 16742155
US_SURVEY_FOOT_M = 1200 / 3937
_TIFF_LITTLE_ENDIAN = bytes((0x49, 0x49, 42, 0))
_TYPE_SIZE = {1: 1, 2: 1, 3: 2, 4: 4, 12: 8}
_TYPE_CODE = {3: 'H', 4: 'I', 12: 'd'}


class GridUnavailable(ValueError):
    pass


class GeoidGrid:
    """Verified NAD83(2011) to NAVD88 geoid undulation grid, N = h - H metres."""

    def __init__(self, path):
        path = Path(path)
        if not path.is_file() or path.stat().st_size != GRID_SIZE:
            raise GridUnavailable('verified_geoid18_grid_required')
        self._bytes = path.read_bytes()
        self.sha256 = hashlib.sha256(self._bytes).hexdigest()
        if self.sha256 != GRID_SHA256:
            raise GridUnavailable('verified_geoid18_grid_required')
        self._tags = self._read_single_ifd()
        self._tiles = {}
        # 32-bit IEEE float, one sample, deflate, floating point predictor.
        if tuple(self._tags[t][0] for t in (258, 259, 277, 317, 339)) != (32, 8, 1, 3, 3):
            raise GridUnavailable('unsupported_grid_encoding')
        keys = self._tags[34735]
        geokeys = {keys[i]: keys[i + 3] for i in range(4, len(keys), 4)}
        # 1025=2 is pixel-is-point; 4096=6319 is the NAD83(2011) ellipsoidal base.
        if geokeys.get(1025) != 2 or geokeys.get(4096) != 6319:
            raise GridUnavailable('unsupported_grid_georeference')
        self.width, self.height = self._tags[256][0], self._tags[257][0]
        self.tile_w, self.tile_h = self._tags[322][0], self._tags[323][0]
        self.step_lon, self.step_lat = self._tags[33550][:2]
        tie = self._tags[33922]
        if tie[:3] != (0.0, 0.0, 0.0):
            raise GridUnavailable('unsupported_grid_georeference')
        self.west, self.north = tie[3], tie[4]

    def _read_single_ifd(self):
        data = self._bytes
        if data[:4] != _TIFF_LITTLE_ENDIAN:
            raise GridUnavailable('unsupported_grid_encoding')
        offset, = struct.unpack_from('<I', data, 4)
        count, = struct.unpack_from('<H', data, offset)
        tags = {}
        for index in range(count):
            entry = offset + 2 + 12 * index
            tag, kind, number = struct.unpack_from('<HHI', data, entry)
            if kind not in _TYPE_CODE:
                continue
            length = _TYPE_SIZE[kind] * number
            at = entry + 8 if length <= 4 else struct.unpack_from('<I', data, entry + 8)[0]
            tags[tag] = struct.unpack_from(f'<{number}{_TYPE_CODE[kind]}', data, at)
        if struct.unpack_from('<I', data, offset + 2 + 12 * count)[0] != 0:
            raise GridUnavailable('unsupported_grid_encoding')
        return tags

    def _tile(self, column, row):
        across = -(-self.width // self.tile_w)
        index = row * across + column
        if index not in self._tiles:
            at, size = self._tags[324][index], self._tags[325][index]
            raw = bytearray(zlib.decompress(self._bytes[at:at + size]))
            stride = self.tile_w * 4
            if len(raw) != stride * self.tile_h:
                raise GridUnavailable('unsupported_grid_encoding')
            rows = []
            for start in range(0, len(raw), stride):
                line = raw[start:start + stride]
                for i in range(1, stride):
                    line[i] = (line[i] + line[i - 1]) & 255
                # The predictor stores byte planes, most significant byte first.
                values = bytearray(stride)
                for plane in range(4):
                    values[plane::4] = line[plane * self.tile_w:(plane + 1) * self.tile_w]
                rows.append(struct.unpack(f'>{self.tile_w}f', bytes(values)))
            self._tiles[index] = rows
        return self._tiles[index]

    def _sample(self, column, row):
        tile = self._tile(column // self.tile_w, row // self.tile_h)
        value = tile[row % self.tile_h][column % self.tile_w]
        if not math.isfinite(value) or abs(value) > 200:
            raise GridUnavailable('grid_value_unavailable')
        return value

    def undulation(self, longitude, latitude):
        if not all(math.isfinite(v) for v in (longitude, latitude)):
            raise ValueError('finite_coordinates_required')
        x = (longitude % 360 - self.west) / self.step_lon
        y = (self.north - latitude) / self.step_lat
        column, row = math.floor(x), math.floor(y)
        if not (0 <= column < self.width - 1 and 0 <= row < self.height - 1):
            raise GridUnavailable('outside_grid_coverage')
        dx, dy = x - column, y - row
        return ((1 - dx) * (1 - dy) * self._sample(column, row)
                + dx * (1 - dy) * self._sample(column + 1, row)
                + (1 - dx) * dy * self._sample(column, row + 1)
                + dx * dy * self._sample(column + 1, row + 1))


class TransverseMercator:
    """GRS80 transverse Mercator, fourth order Krueger series."""

    def __init__(self, lat0, lon0, scale, false_east, false_north, unit=1.0):
        f = 1 / 298.257222101
        n = f / (2 - f)
        self._e = math.sqrt(f * (2 - f))
        self._radius = scale * 6378137.0 / (1 + n) * (1 + n**2 / 4 + n**4 / 64)
        self._alpha = (n / 2 - 2 * n**2 / 3 + 5 * n**3 / 16 + 41 * n**4 / 180,
                       13 * n**2 / 48 - 3 * n**3 / 5 + 557 * n**4 / 1440,
                       61 * n**3 / 240 - 103 * n**4 / 140, 49561 * n**4 / 161280)
        self._beta = (n / 2 - 2 * n**2 / 3 + 37 * n**3 / 96 - n**4 / 360,
                      n**2 / 48 + n**3 / 15 - 437 * n**4 / 1440,
                      17 * n**3 / 480 - 37 * n**4 / 840, 4397 * n**4 / 161280)
        self._lon0, self._unit = lon0, unit
        self._fe, self._fn = false_east, false_north
        self._north0 = self._plane(lat0, lon0)[1]

    def _conformal(self, tau):
        sigma = math.sinh(self._e * math.atanh(self._e * tau / math.hypot(1, tau)))
        return tau * math.hypot(1, sigma) - sigma * math.hypot(1, tau)

    def _plane(self, lat, lon):
        taup = self._conformal(math.tan(math.radians(lat)))
        dlon = math.radians(lon - self._lon0)
        xi0 = math.atan2(taup, math.cos(dlon))
        eta0 = math.asinh(math.sin(dlon) / math.hypot(taup, math.cos(dlon)))
        xi = xi0 + sum(a * math.sin(2 * j * xi0) * math.cosh(2 * j * eta0)
                       for j, a in enumerate(self._alpha, 1))
        eta = eta0 + sum(a * math.cos(2 * j * xi0) * math.sinh(2 * j * eta0)
                         for j, a in enumerate(self._alpha, 1))
        return self._radius * eta, self._radius * xi

    def forward(self, lon, lat):
        east, north = self._plane(lat, lon)
        return ((east + self._fe) / self._unit,
                (north - self._north0 + self._fn) / self._unit)

    def inverse(self, east, north):
        eta = (east * self._unit - self._fe) / self._radius
        xi = (north * self._unit - self._fn + self._north0) / self._radius
        xi0 = xi - sum(b * math.sin(2 * j * xi) * math.cosh(2 * j * eta)
                       for j, b in enumerate(self._beta, 1))
        eta0 = eta - sum(b * math.cos(2 * j * xi) * math.sinh(2 * j * eta)
                         for j, b in enumerate(self._beta, 1))
        target = math.sin(xi0) / math.hypot(math.sinh(eta0), math.cos(xi0))
        tau = target
        for _ in range(8):
            step = 1e-6 * max(1.0, abs(tau))
            slope = (self._conformal(tau + step) - self._conformal(tau - step)) / (2 * step)
            tau += (target - self._conformal(tau)) / slope
        lon = self._lon0 + math.degrees(math.atan2(math.sinh(eta0), math.cos(xi0)))
        return lon, math.degrees(math.atan(tau))


_LAT0, _LON0, _K = 41 + 28 / 60 + 20 / 3600, -(88 + 46 / 60 + 30 / 3600), 1.0000346418
# Published EPSG parameters. 7541 is metres; 7600 states its offsets in US survey feet.
EPSG_7541 = TransverseMercator(_LAT0, _LON0, _K, 263347.7263, 0.0076)
EPSG_7600 = TransverseMercator(_LAT0, _LON0, _K, 863999.999 * US_SURVEY_FOOT_M,
                               0.025 * US_SURVEY_FOOT_M, US_SURVEY_FOOT_M)
