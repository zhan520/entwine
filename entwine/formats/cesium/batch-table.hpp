/******************************************************************************
* Copyright (c) 2017, Connor Manning (connor@hobu.co)
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

#include <entwine/formats/cesium/batch-reference.hpp>
#include <entwine/types/binary-point-table.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/point.hpp>

namespace entwine
{
namespace cesium
{

class TileData;

class BatchTable
{
public:
    BatchTable(const Metadata& metadata, const TileData& tileData);

    Json::Value getJson() const;
    void appendBinary(std::vector<char>& data) const;
    std::size_t bytes() const;

private:
    const std::vector<Point>& points() const;
    const std::vector<Color>& colors() const;
    const std::vector<Point>& normals() const;

    const Metadata& m_metadata;
    const TileData& m_tileData;
    std::vector<BatchReference> m_batchReferences;
    std::vector<char> m_data;

    BinaryPointTable m_table;
    pdal::PointRef m_pointRef;
};

} // namespace cesium
} // namespace entwine

