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

#include <cstddef>
#include <memory>

#include <json/json.h>

#include <entwine/formats/cesium/settings.hpp>
#include <entwine/tree/thread-pools.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

class Config
{
public:
    Config() { }

    Config(const Json::Value& json)
        : m_json(json)
    {
        if (isSubset())
        {
            m_subsetId = makeUnique<std::size_t>(
                    m_json["subset"]["id"].asUInt64());
        }
    }

    std::string output() const { return m_json["output"].asString(); }
    std::string tmp() const { return m_json["tmp"].asString(); }
    std::string storage() const { return m_json["storage"].asString(); }

    std::unique_ptr<cesium::Settings> cesiumSettings() const
    {
        if (isMember("cesium"))
        {
            return makeUnique<cesium::Settings>(m_json["cesium"]);
        }
        return std::unique_ptr<cesium::Settings>();
    }

    bool absolute() const { return m_json["absolute"].asBool(); }
    bool force() const { return m_json["force"].asBool(); }
    bool storePointId() const { return m_json["storePointId"].asBool(); }
    bool trustHeaders() const { return m_json["trustHeaders"].asBool(); }
    bool verbose() const { return m_json["verbose"].asBool(); }

    std::size_t threads() const { return workThreads() + clipThreads(); }

    std::size_t workThreads() const
    {
        const Json::Value threads(m_json["threads"]);
        if (threads.isNull()) return 0;
        if (threads.isArray()) return threads[0].asUInt64();
        return ThreadPools::getWorkThreads(threads.asUInt64());
    }

    std::size_t clipThreads() const
    {
        const Json::Value threads(m_json["threads"]);
        if (threads.isNull()) return 0;
        if (threads.isArray()) return threads[1].asUInt64();
        return ThreadPools::getClipThreads(threads.asUInt64());
    }

    bool isSubset() const { return m_json.isMember("subset"); }
    const std::size_t* subsetId() const { return m_subsetId.get(); }
    std::size_t id() const { return m_json["subset"]["id"].asUInt64(); }
    std::size_t of() const { return m_json["subset"]["of"].asUInt64(); }

    void setSubsetId(std::size_t id)
    {
        m_json["subset"]["id"] = static_cast<Json::UInt64>(id);
        m_subsetId = makeUnique<std::size_t>(id);
    }

    void setSubsetOf(std::size_t of)
    {
        m_json["subset"]["of"] = static_cast<Json::UInt64>(of);
    }

    std::string entwineFile() const
    {
        return "entwine" + Subset::postfix(subsetId());
    }

    const Json::Value arbiter() const { return m_json["arbiter"]; }

    // Plain JSON access.
    Json::Value& operator[](const std::string& k) { return m_json[k]; }
    const Json::Value& operator[](const std::string& k) const
    {
        return m_json[k];
    }
    Json::Value& get() { return m_json; }
    const Json::Value& get() const { return m_json; }
    bool isMember(const std::string& k) const { return m_json.isMember(k); }
    bool has(const std::string& k) const { return isMember(k); }

    void maybeSet(const std::string& k, const Json::Value& v)
    {
        if (!m_json.isMember(k)) m_json[k] = v;
    }

private:
    Json::Value m_json;
    std::shared_ptr<std::size_t> m_subsetId;
};

} // namespace entwine

