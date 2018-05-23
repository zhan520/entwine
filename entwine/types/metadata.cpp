/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <cassert>

#include <entwine/types/chunk-storage/chunk-storage.hpp>
#include <entwine/types/delta.hpp>
#include <entwine/types/files.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/io.hpp>
#include <entwine/util/json.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

Metadata::Metadata(const Config& config, const bool exists)
    : m_delta(makeUnique<Delta>(config.delta()))
    , m_boundsNativeConforming(makeUnique<Bounds>(
                exists ?
                    Bounds(config["boundsConforming"]) :
                    makeNativeConformingBounds(config["bounds"])))
    , m_boundsNativeCubic(makeUnique<Bounds>(
                exists ?
                    Bounds(config["bounds"]) :
                    makeNativeCube(*m_boundsNativeConforming, m_delta.get())))
    , m_boundsScaledConforming(
            clone(m_boundsNativeConforming->deltify(*m_delta)))
    , m_boundsScaledCubic(
            clone(m_boundsNativeCubic->deltify(*m_delta)))
    , m_schema(makeUnique<Schema>(config["schema"]))
    , m_files(makeUnique<Files>(config.input()))
    , m_chunkStorage(ChunkStorage::create(*this, config.dataStorage()))
    , m_reprojection(Reprojection::create(config["reprojection"]))
    , m_version(makeUnique<Version>(currentVersion()))
    , m_srs(config.srs().empty() && m_reprojection ?
            m_reprojection->out() : config.srs())
    , m_subset(Subset::create(*this, config["subset"]))
    , m_density(config.density())
    , m_trustHeaders(config.trustHeaders())
    , m_totalPoints(m_files->totalPoints())
    , m_splits(config["splits"].asUInt64())
    , m_gridSpan(1UL << m_splits)
    , m_sharedDepth(m_subset ? m_subset->splits() : 0)
    , m_overflowDepth(std::max(config.overflowDepth(), m_sharedDepth))
    , m_overflowThreshold(m_gridSpan * m_gridSpan * config.overflowRatio())
{ }

Metadata::Metadata(const arbiter::Endpoint& ep, const Config& config)
    : Metadata(
            entwine::merge(
                config.json(),
                entwine::merge(
                    parse(ep.get("entwine" +
                            config.postfix() + ".json")),
                    parse(ep.get("entwine-params" +
                            config.postfix() + ".json")))),
            true)
{
    Files files(parse(ep.get("entwine-files" + postfix() + ".json")));
    files.append(m_files->list());
    m_files = makeUnique<Files>(files.list());
}

Metadata::~Metadata() { }

Json::Value Metadata::toJson() const
{
    Json::Value json;

    json["bounds"] = boundsNativeCubic().toJson();
    json["boundsConforming"] = boundsNativeConforming().toJson();
    json["schema"] = m_schema->toJson();
    json["splits"] = m_splits;
    json["numPoints"] = m_totalPoints;

    if (m_srs.size()) json["srs"] = m_srs;
    if (m_reprojection) json["reprojection"] = m_reprojection->toJson();

    if (m_delta) json = entwine::merge(json, m_delta->toJson());

    if (m_transformation)
    {
        for (const double v : *m_transformation)
        {
            json["transformation"].append(v);
        }
    }

    json["dataStorage"] = "laszip";
    json["hierarchyStorage"] = "json";

    return json;
}

Json::Value Metadata::toBuildParamsJson() const
{
    Json::Value json;

    json["version"] = m_version->toString();
    json["trustHeaders"] = m_trustHeaders;
    json["overflowDepth"] = Json::UInt64(m_overflowDepth);
    json["overflowThreshold"] = Json::UInt64(m_overflowThreshold);
    if (m_subset) json["subset"] = m_subset->toJson();

    return json;
}

void Metadata::save(const arbiter::Endpoint& endpoint) const
{
    {
        const auto json(toJson());
        const std::string f("entwine" + postfix() + ".json");
        io::ensurePut(endpoint, f, json.toStyledString());
    }

    {
        const auto json(toBuildParamsJson());
        const std::string f("entwine-params" + postfix() + ".json");
        io::ensurePut(endpoint, f, json.toStyledString());
    }

    m_files->save(endpoint, postfix());
}

void Metadata::merge(const Metadata& other)
{
    if (m_srs.empty()) m_srs = other.srs();
    m_files->merge(other.files());
}

void Metadata::makeWhole()
{
    m_subset.reset();
}

std::string Metadata::postfix() const
{
    if (const Subset* s = subset()) return "-" + std::to_string(s->id());
    return "";
}

std::string Metadata::postfix(const uint64_t depth) const
{
    if (const Subset* s = subset())
    {
        if (depth < m_sharedDepth)
        {
            return "-" + std::to_string(s->id());
        }
    }

    return "";
}

Bounds Metadata::makeNativeConformingBounds(const Bounds& b) const
{
    Point pmin(b.min());
    Point pmax(b.max());

    pmin = pmin.apply([](double d)
    {
        if (static_cast<double>(static_cast<uint64_t>(d)) == d) return d - 1.0;
        else return std::floor(d);
    });

    pmax = pmax.apply([](double d)
    {
        if (static_cast<double>(static_cast<uint64_t>(d)) == d) return d + 1.0;
        else return std::ceil(d);
    });

    return Bounds(pmin, pmax);
}

Bounds Metadata::makeNativeCube(const Bounds& b, const Delta& d) const
{
    const double maxDist(std::max(std::max(b.width(), b.depth()), b.height()));
    double r(maxDist / 2.0);

    if (static_cast<double>(static_cast<uint64_t>(r)) == r) r += 1.0;
    else r = std::ceil(r);

    return Bounds(d.offset() - r, d.offset() + r);
}

} // namespace entwine

