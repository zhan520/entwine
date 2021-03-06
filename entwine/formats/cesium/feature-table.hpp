/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <vector>

#include <json/json.h>

#include <entwine/types/point.hpp>

namespace entwine
{
namespace cesium
{

class TileData;

class FeatureTable
{
public:
    FeatureTable(const TileData& tileData);

    Json::Value getJson() const;
    void appendBinary(std::vector<char>& data) const;
    std::size_t bytes() const;

private:
    const std::vector<Point>& points() const;
    const std::vector<Color>& colors() const;
    const std::vector<Point>& normals() const;

    const TileData& m_tileData;
};

} // namespace cesium
} // namespace entwine

