/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <entwine/tree/builder.hpp>

#include <chrono>
#include <limits>
#include <numeric>
#include <random>
#include <thread>

#include <entwine/third/arbiter/arbiter.hpp>
#include <entwine/third/splice-pool/splice-pool.hpp>
#include <entwine/tree/chunk.hpp>
#include <entwine/tree/climber.hpp>
#include <entwine/tree/clipper.hpp>
#include <entwine/tree/config.hpp>
#include <entwine/tree/heuristics.hpp>
#include <entwine/tree/hierarchy-block.hpp>
#include <entwine/tree/registry.hpp>
#include <entwine/tree/sequence.hpp>
#include <entwine/tree/thread-pools.hpp>
#include <entwine/tree/traverser.hpp>
#include <entwine/types/bounds.hpp>
#include <entwine/types/metadata.hpp>
#include <entwine/types/pooled-point-table.hpp>
#include <entwine/types/reprojection.hpp>
#include <entwine/types/schema.hpp>
#include <entwine/types/structure.hpp>
#include <entwine/types/subset.hpp>
#include <entwine/util/compression.hpp>
#include <entwine/util/executor.hpp>
#include <entwine/util/pool.hpp>
#include <entwine/util/unique.hpp>

namespace entwine
{

using namespace arbiter;

namespace
{
    const std::size_t inputRetryLimit(8);
}

/*
Builder::Builder(
        const Metadata& metadata,
        const std::string outPath,
        const std::string tmpPath,
        const std::size_t workThreads,
        const std::size_t clipThreads,
        const OuterScope outerScope)
    : m_arbiter(outerScope.getArbiter())
    , m_outEp(makeUnique<Endpoint>(m_arbiter->getEndpoint(outPath)))
    , m_tmpEp(makeUnique<Endpoint>(m_arbiter->getEndpoint(tmpPath)))
    , m_threadPools(makeUnique<ThreadPools>(workThreads, clipThreads))
    , m_metadata(([this, &metadata]()
    {
        auto m(clone(metadata));
        m->manifest().awakenAll(m_threadPools->clipPool());
        return m;
    })())
    , m_pointPool(
            outerScope.getPointPool(m_metadata->schema(), m_metadata->delta()))
    , m_hierarchyPool(outerScope.getHierarchyPool(heuristics::poolBlockSize))
    , m_hierarchy(makeUnique<Hierarchy>(
                *m_hierarchyPool,
                *m_metadata,
                *m_outEp,
                m_outEp.get(),
                false))
    , m_sequence(makeUnique<Sequence>(*this))
    , m_registry(makeUnique<Registry>(*this))
{
    prepareEndpoints();
}

Builder::Builder(
        const Config& config,
        const std::size_t* subsetId,
        const OuterScope outerScope)
    : m_arbiter(outerScope.getArbiter())
    , m_outEp(makeUnique<Endpoint>(m_arbiter->getEndpoint(config.output())
    , m_tmpEp(makeUnique<Endpoint>(m_arbiter->getEndpoint(config.tmp())))
    , m_threadPools(makeUnique<ThreadPools>(
            config.workThreads(),
            config.clipThreads()))
    , m_metadata(([this, subsetId]()
    {
        auto m(makeUnique<Metadata>(*m_outEp, subsetId));
        m->manifest().awakenAll(m_threadPools->clipPool());
        return m;
    })())
    , m_pointPool(
            outerScope.getPointPool(m_metadata->schema(), m_metadata->delta()))
    , m_hierarchyPool(outerScope.getHierarchyPool(heuristics::poolBlockSize))
    , m_hierarchy(makeUnique<Hierarchy>(
                *m_hierarchyPool,
                *m_metadata,
                *m_outEp,
                m_outEp.get(),
                true))
    , m_sequence(makeUnique<Sequence>(*this))
    , m_registry(makeUnique<Registry>(*this, true))
{
    prepareEndpoints();
}
*/

Builder::Builder(const Config& config, const OuterScope outerScope)
    : m_config(config)
    , m_arbiter(outerScope.getArbiter())
    , m_outEp(makeUnique<Endpoint>(m_arbiter->getEndpoint(m_config.output())))
    , m_tmpEp(makeUnique<Endpoint>(m_arbiter->getEndpoint(m_config.tmp())))
    , m_exists(!m_config.force() && m_outEp->tryGetSize(m_config.entwineFile()))
    , m_threadPools(makeUnique<ThreadPools>(
                m_config.workThreads(),
                m_config.clipThreads()))
    , m_metadata(([this]()
    {
        auto m = m_exists ?
            makeUnique<Metadata>(*m_outEp, m_config.subsetId()) :
            makeUnique<Metadata>(m_config);
        m->manifest().awakenAll(m_threadPools->clipPool());
        return m;
    })())
    , m_pointPool(
            outerScope.getPointPool(m_metadata->schema(), m_metadata->delta()))
    , m_hierarchyPool(outerScope.getHierarchyPool(heuristics::poolBlockSize))
    , m_hierarchy(makeUnique<Hierarchy>(
                *m_hierarchyPool,
                *m_metadata,
                *m_outEp,
                m_outEp.get(),
                m_exists))
    , m_sequence(makeUnique<Sequence>(*this))
    , m_registry(makeUnique<Registry>(*this, m_exists))
    , m_verbose(m_config.verbose())
{
    prepareEndpoints();
}

std::unique_ptr<Builder> Builder::tryCreateExisting(
        const Config& config,
        OuterScope os)
{
    if (!config.force())
    {
        const std::string file(config.entwineFile());
        if (os.getArbiter()->getEndpoint(config.output()).tryGetSize(file))
        {
            return makeUnique<Builder>(config, os);
        }
    }

    return std::unique_ptr<Builder>();
}

Builder::~Builder()
{ }

void Builder::go(std::size_t max)
{
    if (!m_tmpEp)
    {
        throw std::runtime_error("Cannot add to read-only builder");
    }

    while (auto o = m_sequence->next(max))
    {
        const Origin origin(*o);
        FileInfo& info(m_metadata->manifest().get(origin));
        const auto path(info.path());

        if (verbose())
        {
            std::cout << "Adding " << origin << " - " << path << std::endl;
            std::cout <<
                " A: " << m_pointPool->cellPool().allocated() <<
                " C: " << Chunk::count() <<
                " H: " << HierarchyBlock::count() <<
                std::endl;
        }

        m_threadPools->workPool().add([this, origin, &info, path]()
        {
            FileInfo::Status status(FileInfo::Status::Inserted);
            std::string message;

            try
            {
                insertPath(origin, info);
            }
            catch (const std::exception& e)
            {
                if (verbose())
                {
                    std::cout << "During " << path << ": " << e.what() <<
                        std::endl;
                }

                status = FileInfo::Status::Error;
                message = e.what();
            }
            catch (...)
            {
                if (verbose())
                {
                    std::cout << "Unknown error during " << path << std::endl;
                }

                status = FileInfo::Status::Error;
                message = "Unknown error";
            }

            m_metadata->manifest().set(origin, status, message);
        });
    }

    if (verbose())
    {
        std::cout << "\tPushes complete - joining..." << std::endl;
    }

    save();
}

bool Builder::insertPath(const Origin origin, FileInfo& info)
{
    const auto rawPath(info.path());
    std::size_t tries(0);
    std::unique_ptr<arbiter::fs::LocalHandle> localHandle;

    do
    {
        try
        {
            localHandle = m_arbiter->getLocalHandle(rawPath, *m_tmpEp);
        }
        catch (const ArbiterError& e)
        {
            if (verbose())
            {
                std::cout <<
                    "Failed GET attempt of " << rawPath << ": " << e.what() <<
                    std::endl;
            }

            localHandle.reset();
            std::this_thread::sleep_for(std::chrono::seconds(tries));
        }
    }
    while (!localHandle && ++tries < inputRetryLimit);

    const std::string& localPath(localHandle->localPath());

    const Reprojection* reprojection(m_metadata->reprojection());
    const Transformation* transformation(m_metadata->transformation());

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string& srs(m_metadata->srs());

        if (srs.empty())
        {
            auto preview(Executor::get().preview(localPath, nullptr));
            if (preview) srs = preview->srs;

            if (verbose() && srs.size())
            {
                std::cout << "Found an SRS" << std::endl;
            }
        }
    }

    std::size_t inserted(0);

    Clipper clipper(*this, origin);
    Climber climber(*m_metadata, m_hierarchy.get());

    auto inserter([this, origin, &clipper, &climber, &inserted]
    (Cell::PooledStack cells)
    {
        inserted += cells.size();

        if (inserted > heuristics::sleepCount)
        {
            inserted = 0;
            clipper.clip();
        }

        return insertData(std::move(cells), origin, clipper, climber);
    });

    std::unique_ptr<PooledPointTable> table(
            PooledPointTable::create(
                *m_pointPool,
                inserter,
                m_metadata->delta(),
                origin));

    return Executor::get().run(
            *table,
            localPath,
            reprojection,
            transformation,
            m_metadata->preserveSpatial());
}

Cell::PooledStack Builder::insertData(
        Cell::PooledStack cells,
        const Origin origin,
        Clipper& clipper,
        Climber& climber)
{
    PointStats pointStats;
    Cell::PooledStack rejected(m_pointPool->cellPool());

    auto reject([&rejected](Cell::PooledNode& cell)
    {
        rejected.push(std::move(cell));
    });

    const Bounds& boundsConforming(m_metadata->boundsScaledEpsilon());
    const auto boundsSubset(m_metadata->boundsScaledSubset());
    const std::size_t baseDepthBegin(m_metadata->structure().baseDepthBegin());

    while (!cells.empty())
    {
        Cell::PooledNode cell(cells.popOne());
        const Point& point(cell->point());

        if (boundsConforming.contains(point))
        {
            if (!boundsSubset || boundsSubset->contains(point))
            {
                climber.reset();
                climber.magnifyTo(point, baseDepthBegin);

                if (m_registry->addPoint(cell, climber, clipper))
                {
                    pointStats.addInsert();
                }
                else
                {
                    reject(cell);
                    pointStats.addOverflow();
                }
            }
            else
            {
                reject(cell);
            }
        }
        else
        {
            reject(cell);
            pointStats.addOutOfBounds();
        }
    }

    if (origin != invalidOrigin) m_metadata->manifest().add(origin, pointStats);
    return rejected;
}

void Builder::save()
{
    save(*m_outEp);
}

void Builder::save(const std::string to)
{
    save(m_arbiter->getEndpoint(to));
}

void Builder::save(const arbiter::Endpoint& ep)
{
    m_threadPools->cycle();

    if (verbose()) std::cout << "Saving hierarchy..." << std::endl;
    m_hierarchy->save(m_threadPools->clipPool());

    if (verbose()) std::cout << "Saving registry..." << std::endl;
    m_registry->save(*m_outEp);

    if (verbose()) std::cout << "Saving metadata..." << std::endl;
    m_metadata->save(*m_outEp);
}

void Builder::merge(Builder& other)
{
    if (!m_metadata->subset())
    {
        throw std::runtime_error("Cannot merge non-subset build");
    }

    if (m_threadPools->clipPool().running())
    {
        m_threadPools->workPool().resize(m_threadPools->size());
        m_threadPools->clipPool().join();
    }

    m_registry->merge(*other.m_registry);
    m_metadata->merge(*other.m_metadata);
    m_hierarchy->merge(*other.m_hierarchy, m_threadPools->workPool());
}

void Builder::prepareEndpoints()
{
    if (m_tmpEp)
    {
        if (m_tmpEp->isRemote())
        {
            throw std::runtime_error("Tmp path must be local");
        }

        if (!arbiter::fs::mkdirp(m_tmpEp->root()))
        {
            throw std::runtime_error("Couldn't create tmp directory");
        }

        const std::string rootDir(m_outEp->root());
        if (!m_outEp->isRemote())
        {
            if (!arbiter::fs::mkdirp(rootDir))
            {
                throw std::runtime_error("Couldn't create " + rootDir);
            }

            if (!arbiter::fs::mkdirp(rootDir + "h"))
            {
                throw std::runtime_error("Couldn't create " + rootDir + "h");
            }

            if (!arbiter::fs::mkdirp(rootDir + "laz"))
            {
                throw std::runtime_error("Couldn't create " + rootDir + "h");
            }

            if (
                    m_metadata->cesiumSettings() &&
                    !arbiter::fs::mkdirp(rootDir + "cesium"))
            {
                throw std::runtime_error(
                        "Couldn't create " + rootDir + "cesium");
            }
        }
    }
}

void Builder::unbump() { m_metadata->unbump(); }
void Builder::makeWhole() { m_metadata->makeWhole(); }

const Metadata& Builder::metadata() const           { return *m_metadata; }
const Registry& Builder::registry() const           { return *m_registry; }
const Hierarchy& Builder::hierarchy() const         { return *m_hierarchy; }
const arbiter::Arbiter& Builder::arbiter() const    { return *m_arbiter; }
arbiter::Arbiter& Builder::arbiter() { return *m_arbiter; }

Sequence& Builder::sequence() { return *m_sequence; }
const Sequence& Builder::sequence() const { return *m_sequence; }

ThreadPools& Builder::threadPools() const { return *m_threadPools; }

PointPool& Builder::pointPool() const { return *m_pointPool; }
std::shared_ptr<PointPool> Builder::sharedPointPool() const
{
    return m_pointPool;
}

std::shared_ptr<HierarchyCell::Pool> Builder::sharedHierarchyPool() const
{
    return m_hierarchyPool;
}

const arbiter::Endpoint& Builder::outEndpoint() const { return *m_outEp; }
const arbiter::Endpoint& Builder::tmpEndpoint() const { return *m_tmpEp; }

std::mutex& Builder::mutex() { return m_mutex; }

void Builder::append(const FileInfoList& fileInfo)
{
    m_metadata->manifest().append(fileInfo);
    m_sequence = makeUnique<Sequence>(*this);
}

void Builder::clip(
        const Id& index,
        const std::size_t chunkNum,
        const std::size_t id,
        const bool sync)
{
    m_registry->clip(index, chunkNum, id, sync);
}

} // namespace entwine

